const mongoose = require('mongoose');
const { ObjectId } = require('mongodb');

/* ----------------------------- helpers ----------------------------------- */

function toInt(v) {
  if (typeof v === 'string') return parseInt(v, 10);
  return v;
}
function buildUserMatch(pmwbUserId, pmwebProjectId) {
  const match = { PMWBUserId: toInt(pmwbUserId) };
  if (pmwebProjectId != null) match.PMWEBProjectId = toInt(pmwebProjectId);
  return match;
}
function toObjectIds(ids) {
  return (Array.isArray(ids) ? ids : [])
    .filter(Boolean)
    .map(id => (mongoose.Types.ObjectId.isValid(id) ? new mongoose.Types.ObjectId(id) : id));
}

/* ------------------------------ core queries ----------------------------- */

/**
 * Flattened rows for a user's visibility:
 * projectusers → sgprojects → projectstagegates (active) → process → task → activity → subactivity
 * + projectphaseassetdetails (Community/Phase)
 *
 * NOTE: Stagegates/processes are unwound with preserveNull so phases show up even if empty.
 */
async function getStagegateHierarchyRows(pmwbUserId, opts = {}) {
  const db = mongoose.connection.db;
  const users = db.collection('projectusers');

  const pipeline = [
    { $match: buildUserMatch(pmwbUserId, opts.pmwebProjectId) },

    // Link projects (sgprojects)
    {
      $lookup: {
        from: 'sgprojects',
        localField: 'PMWEBProjectId',
        foreignField: 'PMWEB_ProjectId',
        as: 'project'
      }
    },
    { $unwind: '$project' }, // keep strict here: we scope using sgprojects

    // Community / Phase labels
    {
      $lookup: {
        from: 'projectphaseassetdetails',
        localField: 'PMWEBProjectId',
        foreignField: 'PMWEB_ProjectId',
        as: 'phaseDetails'
      }
    },
    { $unwind: { path: '$phaseDetails', preserveNullAndEmptyArrays: true } },

    // Active stagegates for this sgproject (preserve even if none)
    {
      $lookup: {
        from: 'projectstagegates',
        let: { projId: '$project._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectId', '$$projId'] } } },
          { $match: { ProjectStagegateIsActive: true } }
        ],
        as: 'stagegates'
      }
    },
    { $unwind: { path: '$stagegates', preserveNullAndEmptyArrays: true } },

    // SG → Process (preserve even if none)
    {
      $lookup: {
        from: 'projectprocess',
        localField: 'stagegates._id',
        foreignField: 'ProjectStagegateId',
        as: 'processes'
      }
    },
    { $unwind: { path: '$processes', preserveNullAndEmptyArrays: true } },

    // Process → Task (optional)
    {
      $lookup: {
        from: 'projecttask',
        localField: 'processes._id',
        foreignField: 'ProjectProcessId',
        as: 'tasks'
      }
    },
    { $unwind: { path: '$tasks', preserveNullAndEmptyArrays: true } },

    // Task → Activity (optional)
    {
      $lookup: {
        from: 'projectactivity',
        localField: 'tasks._id',
        foreignField: 'ProjectTaskId',
        as: 'activities'
      }
    },
    { $unwind: { path: '$activities', preserveNullAndEmptyArrays: true } },

    // Activity → SubActivity (optional)
    {
      $lookup: {
        from: 'projectsubactivity',
        localField: 'activities._id',
        foreignField: 'ProjectActivityId',
        as: 'subactivities'
      }
    },
    { $unwind: { path: '$subactivities', preserveNullAndEmptyArrays: true } },

    // Final flattened row
    {
      $project: {
        _id: 0,
        userId: '$PMWBUserId',
        pmwebProjectId: '$project.PMWEB_ProjectId',
        projectId: '$project._id',
        projectName: '$project.PMWEB_ProjectName',

        community: {
          $ifNull: [
            '$phaseDetails.Community',
            { $ifNull: ['$project.ProjectCommunity', '$project.PMWEB_ProjectName'] }
          ]
        },
        phaseName: { $ifNull: ['$phaseDetails.Project', '$project.ProjectPhaseOrProject'] },

        stagegateId: '$stagegates._id',
        stagegateName: '$stagegates.ProjectStagegateName',

        processId: '$processes._id',
        processName: '$processes.ProjectProcessName',

        taskId: '$tasks._id',
        taskName: '$tasks.ProjectTaskName',

        activityId: '$activities._id',
        activityName: '$activities.ProjectActivityName',

        subactivityId: '$subactivities._id',
        subactivityName: '$subactivities.ProjectSubActivityName'
      }
    }
  ];

  return users.aggregate(pipeline, { allowDiskUse: true }).toArray();
}

/**
 * Checklist IDs for owners (process/task/activity/subactivity)
 */
async function getChecklistIdsParallel({ processIds, taskIds, activityIds, subactivityIds }) {
  const db = mongoose.connection.db;

  const cfgs = [
    { collection: 'projectprocesschecklist',     field: 'ProcessId',     ids: processIds,     key: 'byProcess' },
    { collection: 'projecttaskchecklist',        field: 'TaskId',        ids: taskIds,        key: 'byTask' },
    { collection: 'projectactivitychecklist',    field: 'ActivityId',    ids: activityIds,    key: 'byActivity' },
    { collection: 'projectsubactivitychecklist', field: 'SubActivityId', ids: subactivityIds, key: 'bySubactivity' },
  ];

  const result = { byProcess: {}, byTask: {}, byActivity: {}, bySubactivity: {}, all: new Set() };

  await Promise.all(cfgs.map(async ({ collection, field, ids, key }) => {
    const list = toObjectIds(ids);
    if (!list.length) return;

    const rows = await db.collection(collection)
      .find({ [field]: { $in: list } }, { projection: { _id: 1, [field]: 1 } })
      .toArray();

    for (const row of rows) {
      const owner = String(row[field]);
      const cid = String(row._id);
      if (!result[key][owner]) result[key][owner] = [];
      result[key][owner].push(cid);
      result.all.add(cid);
    }
  }));

  return result;
}

/**
 * Files across all *files collections for given checklist IDs.
 * Uses FileId (from files docs) as the surfaced fileId so the controller's keys match filecontent._id.
 */
async function getFilesByChecklistIds(checklistIds = []) {
  if (!Array.isArray(checklistIds) || checklistIds.length === 0) return [];

  const db = mongoose.connection.db;
  const fileCollections = [
    'projectprocessfiles',
    'projecttaskfiles',
    'projectactivityfiles',
    'projectsubactivityfiles'
  ];
  const allowed = ['.docx', '.pdf', '.xlsx'];

  const queryIds = toObjectIds(checklistIds);

  const batches = await Promise.all(
    fileCollections.map(col =>
      db.collection(col).find(
        { ChecklistId: { $in: queryIds } },
        // include FileId so we can surface it as the key
        { projection: { _id: 1, ChecklistId: 1, FileId: 1, FileName: 1, name: 1 } }
      ).toArray()
    )
  );

  const rows = [];
  for (const list of batches) {
    for (const r of list) {
      const fileName = r.FileName || r.name;
      if (!fileName) continue;
      const lower = String(fileName).toLowerCase();
      if (!allowed.some(ext => lower.endsWith(ext))) continue;

      rows.push({
        fileId: String(r.FileId ?? r.fileId ?? r._id), // prefer FileId; fallback to row _id
        docId: String(r._id),                           // optional (collection row id)
        checklistId: String(r.ChecklistId),
        fileName
      });
    }
  }
  return rows;
}

/**
 * Threads helper (unchanged).
 */
async function getThreadsByChecklistIds(checklistIdStrs = []) {
  if (!Array.isArray(checklistIdStrs) || checklistIdStrs.length === 0) return [];

  const db = mongoose.connection.db;
  const threads = db.collection('projectThreads');

  const pipeline = [
    { $match: { $expr: { $in: [{ $toString: '$checkListId' }, checklistIdStrs] } } },
    {
      $project: {
        _id: 0,
        checklistIdStr: { $toString: '$checkListId' },
        createdBy: 1,
        participants: { $ifNull: ['$participants', []] },
        attachments: {
          $concatArrays: [
            { $ifNull: ['$sampleAttachments', { $ifNull: ['$sampleAttachements', []] }] },
            { $ifNull: ['$threadAttachments', { $ifNull: ['$threadAttachements', []] }] }
          ]
        }
      }
    }
  ];

  return threads.aggregate(pipeline, { allowDiskUse: true }).toArray();
}

/**
 * filecontent by IDs (kept for future; not used now).
 */
async function getFileContentMetaByIdsOld(fileIds = []) {
  if (!Array.isArray(fileIds) || fileIds.length === 0) return {};

  const objIds = [];
  for (const raw of fileIds) {
    const s = String(raw);
    if (ObjectId.isValid(s)) objIds.push(new ObjectId(s));
  }
  if (!objIds.length) return {};

  const db = mongoose.connection.db;
  const coll = db.collection('filecontent');

  const docs = await coll.find(
    { _id: { $in: objIds } },
    { projection: { FileContent: 1, filecontent: 1, CreatedDate: 1 } }
  ).toArray();

  const out = {};
  for (const d of docs) {
    let content = d.filecontent ?? d.FileContent ?? null;

    if (content && typeof content !== 'string') {
      if (Buffer.isBuffer(content)) {
        content = content.toString('base64');
      } else if (content?.buffer) {
        content = Buffer.from(content.buffer).toString('base64');
      } else {
        content = String(content);
      }
    }

    out[String(d._id)] = {
      filecontent: content,
      CreatedDate: d.CreatedDate ?? null
    };
  }

  return out;
}

async function fetchAsBase64FromSignedUrl(signedUrl) {
  const resp = await fetch(signedUrl, { method: 'GET' });
  if (!resp.ok) {
    const txt = await resp.text().catch(() => '');
    throw new Error(`GCS fetch failed ${resp.status}: ${txt}`);
  }
  const ab = await resp.arrayBuffer();
  const buf = Buffer.from(ab);
  return {
    base64: buf.toString('base64'),
    contentType: resp.headers.get('content-type') || 'application/octet-stream',
    size: Number(resp.headers.get('content-length') || buf.length),
  };
}

function normalizeToBase64(content) {
  if (!content) return null;
  if (typeof content === 'string') return content;                    // already base64 string
  if (Buffer.isBuffer(content)) return content.toString('base64');    // raw buffer
  if (content?.buffer) return Buffer.from(content.buffer).toString('base64'); // BSON Binary
  return String(content);
}

async function getFileContentMetaByIds(fileIds = []) {
  if (!Array.isArray(fileIds) || fileIds.length === 0) return {};

  const objIds = [];
  for (const raw of fileIds) {
    const s = String(raw);
    if (ObjectId.isValid(s)) objIds.push(new ObjectId(s));
  }
  if (!objIds.length) return {};

  const db = mongoose.connection.db;
  const coll = db.collection('filecontent');

  // Pull both old (binary) and new (GCS) fields
  const docs = await coll.find(
    { _id: { $in: objIds } },
    {
      projection: {
        FileName: 1,
        FileType: 1,
        FileSize: 1,
        Bucket: 1,
        GCSPath: 1,
        CreatedDate: 1,
        FileContent: 1,
        filecontent: 1,
      },
    }
  ).toArray();

  // Build results concurrently when we need to hit GCS
  const tasks = docs.map(async (d) => {
    const idStr = String(d._id);

    // 1) Old model: binary in Mongo
    const inline = d.filecontent ?? d.FileContent;
    const inlineBase64 = normalizeToBase64(inline);
    if (inlineBase64) {
      return [
        idStr,
        {
          filecontent: inlineBase64,
          CreatedDate: d.CreatedDate ?? null,
          FileType: d.FileType || 'application/octet-stream',
          FileSize: d.FileSize || (inline?.length ?? null),
          FileName: d.FileName || null,
          source: 'mongo-binary',
        },
      ];
    }

    // 2) New model: stored in GCS (Bucket + GCSPath)
    // if (d.Bucket && d.GCSPath) {
    //   try {
    //     const signedUrl = await getReadSignedUrl(d.Bucket, d.GCSPath, d.FileType);
    //     const { base64, contentType, size } = await fetchAsBase64FromSignedUrl(signedUrl);
    //     return [
    //       idStr,
    //       {
    //         filecontent: base64,
    //         CreatedDate: d.CreatedDate ?? null,
    //         FileType: contentType || d.FileType || 'application/octet-stream',
    //         FileSize: size || d.FileSize || null,
    //         FileName: d.FileName || null,
    //         Bucket: d.Bucket,
    //         GCSPath: d.GCSPath,
    //         source: 'gcs-signed-url',
    //       },
    //     ];
    //   } catch (e) {
    //     console.error(`GCS fetch failed for ${idStr} (${d.Bucket}/${d.GCSPath})`, e);
    //     return [idStr, { error: 'GCS fetch failed', CreatedDate: d.CreatedDate ?? null }];
    //   }
    // }

    // 3) Neither present
    return [idStr, { error: 'No content found', CreatedDate: d.CreatedDate ?? null }];
  });

  const entries = await Promise.all(tasks);
  const out = {};
  for (const [k, v] of entries) out[k] = v;
  return out;
}

/**
 * filecontent by FileName (fallback) — kept for future.
 */
async function getFileContentMetaByFileNames(fileNames = []) {
  if (!Array.isArray(fileNames) || fileNames.length === 0) return {};

  const names = [...new Set(fileNames.filter(Boolean).map(String))];
  if (!names.length) return {};

  const db = mongoose.connection.db;
  const coll = db.collection('filecontent');

  const docs = await coll.find(
    { FileName: { $in: names } },
    { projection: { FileName: 1, FileContent: 1, filecontent: 1, CreatedDate: 1 } }
  ).toArray();

  const out = {};
  for (const d of docs) {
    let content = d.filecontent ?? d.FileContent ?? null;

    if (content && typeof content !== 'string') {
      if (Buffer.isBuffer(content)) {
        content = content.toString('base64');
      } else if (content?.buffer) {
        content = Buffer.from(content.buffer).toString('base64');
      } else {
        content = String(content);
      }
    }

    out[String(d.FileName)] = {
      filecontent: content,
      CreatedDate: d.CreatedDate ?? null
    };
  }

  return out;
}

/* ---------------- user-raci: only show user-assigned items ---------------- */

/**
 * Returns Sets of owner IDs (process/task/activity/subactivity) that the given PMWEB user
 * is explicitly assigned to via raciassigness.assigneeId.
 *
 * raciassigness: { assigneeId, raciGroup, raciId }  (field names may vary; we coalesce)
 * Then joins to the appropriate project* r a c i collection to map RaciId → ownerId.
 */

async function getUserAssignedOwnerIds(pmwbUserId) {
  const db = mongoose.connection.db;
  const assignee = toInt(pmwbUserId);

  // NOTE: use your exact collection name as in your DB (spelling per screenshots)
  const assignColl = db.collection('raciassigness');

  const assignments = await assignColl.find(
    {
      $or: [{ assigneeId: assignee }, { AssigneeId: assignee }],
    },
    { projection: { raciId: 1, RaciId: 1, raciGroup: 1, RaciGroup: 1 } }
  ).toArray();

  const idsByGroup = { process: [], task: [], activity: [], subactivity: [] };
  for (const a of assignments) {
    const group = String(a.raciGroup ?? a.RaciGroup ?? '').toLowerCase();
    const raciId = a.raciId ?? a.RaciId;
    if (!raciId) continue;

    if (group.includes('process')) idsByGroup.process.push(raciId);
    else if (group.includes('task')) idsByGroup.task.push(raciId);
    else if (group.includes('sub')) idsByGroup.subactivity.push(raciId);
    else if (group.includes('activity')) idsByGroup.activity.push(raciId);
  }

  const result = {
    process: new Set(),
    task: new Set(),
    activity: new Set(),
    subactivity: new Set(),
  };

  if (idsByGroup.process.length) {
    const rows = await db.collection('projectprocessraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.process) } }, { projection: { ProcessId: 1 } })
      .toArray();
    rows.forEach(r => r?.ProcessId && result.process.add(String(r.ProcessId)));
  }
  if (idsByGroup.task.length) {
    const rows = await db.collection('projecttaskraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.task) } }, { projection: { TaskId: 1 } })
      .toArray();
    rows.forEach(r => r?.TaskId && result.task.add(String(r.TaskId)));
  }
  if (idsByGroup.activity.length) {
    const rows = await db.collection('projectactivityraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.activity) } }, { projection: { ActivityId: 1 } })
      .toArray();
    rows.forEach(r => r?.ActivityId && result.activity.add(String(r.ActivityId)));
  }
  if (idsByGroup.subactivity.length) {
    const rows = await db.collection('projectsubactivityraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.subactivity) } }, { projection: { SubActivityId: 1 } })
      .toArray();
    rows.forEach(r => r?.SubActivityId && result.subactivity.add(String(r.SubActivityId)));
  }

  return result;
}
//insering comments
async function insertFileComment({ fileId, userId, createdBy, text, page, anchorText, rect  ,yPct ,xPct }) {
  
  const db = mongoose.connection.db;
  const coll = db.collection('projectFileComments');
  const users = db.collection('users');

  const toObjId = s => (ObjectId.isValid(String(s)) ? new ObjectId(String(s)) : null);
  const isDigits = s => typeof s === 'string' && /^[0-9]+$/.test(s.trim());

  const fileIdObj = toObjId(fileId);
  if (!fileIdObj) throw new Error('Validation: fileId must be a valid ObjectId hex string');

  async function resolveUser(ref) {
    if (ref == null) return null;

    const oid = toObjId(ref);
    if (oid) {
      const u = await users.findOne({ _id: oid }, { projection: { _id: 1, pmwebUserID: 1 } });
      return u ? { _id: u._id, pmwebUserID: u.pmwebUserID } : null;
    }

    const str = String(ref).trim();
    const or = [];
    if (isDigits(str)) {
      const n = parseInt(str, 10);
      or.push({ pmwebUserID: n });
    }
    if (str.includes('@')) or.push({ pmwebUserEmail: str });
    or.push({ userName: str });

    const u = await users.findOne({ $or: or }, { projection: { _id: 1, pmwebUserID: 1 } });
    return u ? { _id: u._id, pmwebUserID: u.pmwebUserID } : null;
  }

  const author  = await resolveUser(userId);
  const creator = (await resolveUser(createdBy)) || author;

  if (!author)  throw new Error(`Validation: unable to resolve userId "${userId}" to 'projectusers'.`);
  if (!creator) throw new Error(`Validation: unable to resolve createdBy "${createdBy}".`);

  const now = new Date();
  const doc = {
    fileId: fileIdObj

  ,yPct:yPct ,xPct:xPct ,
    userId: author._id,
    createdBy: creator._id,
    text: String(text),
  
    ...(Number.isFinite(author.pmwebUserID) ? { pmwebUserID: author.pmwebUserID } : {}),
    page: Number.isFinite(page) ? page : undefined,
    anchorText: anchorText || undefined,
    rect: rect || undefined,
    createdAt: now,
    updatedAt: now,
  };



  const res = await coll.insertOne(doc);
  return { _id: res.insertedId, ...doc };

}

async function getFileCommentsByFileId(fileId) {
  if (!fileId) return [];
  const db = mongoose.connection.db;
  const coll = db.collection('projectFileComments');

  const s = String(fileId);
  const ors = [];
  if (ObjectId.isValid(s)) ors.push({ fileId: new ObjectId(s) }); 
  ors.push({ fileIdStr: s });                                     

  const pipeline = [
    { $match: { $or: ors } },
    { $sort: { createdAt: 1, _id: 1 } },

   
    {
      $lookup: {
        from: 'users',
        localField: 'userId',
        foreignField: '_id',
        as: 'user'
      }
    },
    { $unwind: { path: '$user', preserveNullAndEmptyArrays: true } },

    {
      $addFields: {
        pmwebUserID: { $ifNull: ['$pmwebUserID', '$user.pmwebUserID'] },
        userDisplayName: {
          $ifNull: [
            '$user.userName',
            {
              $trim: {
                input: {
                  $concat: [
                    { $ifNull: ['$user.firstName', ''] },
                    ' ',
                    { $ifNull: ['$user.lastName', ''] }
                  ]
                }
              }
            }
          ]
        },
        userEmail: '$user.pmwebUserEmail'
      }
    },

    {
      $project: {
        text: 1, page: 1, anchorText: 1, rect: 1
        ,yPct:1 ,xPct :1,
        fileId: 1, fileIdStr: 1,
        userId: 1, createdBy: 1,
        pmwebUserID: 1,
        createdAt: 1, updatedAt: 1,
        userDisplayName: 1, userEmail: 1
      }
    }
  ];

  return coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
}
/* --------------------------------- exports -------------------------------- */

module.exports = {
  getStagegateHierarchyRows,
  getChecklistIdsParallel,
  getFilesByChecklistIds,
  getThreadsByChecklistIds,
  getFileContentMetaByIds,
  getFileContentMetaByFileNames,
  getUserAssignedOwnerIds, 
  insertFileComment,
  getFileCommentsByFileId
};
