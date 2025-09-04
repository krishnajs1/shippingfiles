
const mongoose = require('mongoose');
const { ObjectId } = require('mongodb');


const DEFAULTS = {
  maxTimeMS: parseInt(process.env.MONGO_OP_MAX_MS || '25000', 10),
};

const HINTS = {
  sgprojects: { PMWEB_ProjectId: 1 },
  projectphaseassetdetails: { PMWEB_ProjectId: 1 },
  projectstagegates: { ProjectId: 1, ProjectStagegateIsActive: 1 },
  projectprocess: { ProjectStagegateId: 1 },
  projecttask: { ProjectProcessId: 1 },
  projectactivity: { ProjectTaskId: 1 },
  projectsubactivity: { ProjectActivityId: 1 },

  projectprocesschecklist: { ProcessId: 1 },
  projecttaskchecklist: { TaskId: 1 },
  projectactivitychecklist: { ActivityId: 1 },
  projectsubactivitychecklist: { SubActivityId: 1 },

  projectprocessfiles: { ChecklistId: 1 },
  projecttaskfiles: { ChecklistId: 1 },
  projectactivityfiles: { ChecklistId: 1 },
  projectsubactivityfiles: { ChecklistId: 1 },

  filecontent_byId: { _id: 1 },
  filecontent_byName: { FileName: 1 },
};

const ALLOWED_FILE_EXTS = new Set(['.docx', '.pdf', '.xlsx']);


function toInt(v) { return typeof v === 'string' ? parseInt(v, 10) : v; }
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
function getDb(opts) { return (opts && opts.db) ? opts.db : mongoose.connection.db; }
function aggOpts(opts, comment) {
  return { allowDiskUse: true, maxTimeMS: (opts && opts.maxTimeMS) || DEFAULTS.maxTimeMS, comment };
}
function findOpts(opts) { return { maxTimeMS: (opts && opts.maxTimeMS) || DEFAULTS.maxTimeMS }; }
function hasAllowedExt(name) {
  const s = String(name || '').toLowerCase();
  for (const ext of ALLOWED_FILE_EXTS) if (s.endsWith(ext)) return true;
  return false;
}


async function getStagegateHierarchyRows(pmwbUserId, opts = {}) {
  const db = getDb(opts);
  const users = db.collection('projectusers');

  const pipeline = [
    { $match: buildUserMatch(pmwbUserId, opts.pmwebProjectId) },

    // sgprojects (sub-pipeline + projection)
    {
      $lookup: {
        from: 'sgprojects',
        let: { projId: '$PMWEBProjectId' },
        pipeline: [
          { $match: { $expr: { $eq: ['$PMWEB_ProjectId', '$$projId'] } } },
          { $project: { PMWEB_ProjectId: 1, PMWEB_ProjectName: 1, ProjectCommunity: 1, ProjectPhaseOrProject: 1 } },
        ],
        as: 'project',
      }
    },
    { $unwind: '$project' },

    // projectphaseassetdetails
    {
      $lookup: {
        from: 'projectphaseassetdetails',
        let: { projId: '$PMWEBProjectId' },
        pipeline: [
          { $match: { $expr: { $eq: ['$PMWEB_ProjectId', '$$projId'] } } },
          { $project: { PMWEB_ProjectId: 1, Community: 1, Project: 1 } },
        ],
        as: 'phaseDetails'
      }
    },
    { $unwind: { path: '$phaseDetails', preserveNullAndEmptyArrays: true } },

    // projectstagegates (active)
    {
      $lookup: {
        from: 'projectstagegates',
        let: { projObjId: '$project._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectId', '$$projObjId'] } } },
          { $match: { ProjectStagegateIsActive: true } },
          { $project: { ProjectId: 1, ProjectStagegateName: 1 } },
        ],
        as: 'stagegates'
      }
    },
    { $unwind: { path: '$stagegates', preserveNullAndEmptyArrays: true } },

    // process
    {
      $lookup: {
        from: 'projectprocess',
        let: { sgId: '$stagegates._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectStagegateId', '$$sgId'] } } },
          { $project: { ProjectStagegateId: 1, ProjectProcessName: 1 } },
        ],
        as: 'processes'
      }
    },
    { $unwind: { path: '$processes', preserveNullAndEmptyArrays: true } },

    // task
    {
      $lookup: {
        from: 'projecttask',
        let: { procId: '$processes._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectProcessId', '$$procId'] } } },
          { $project: { ProjectProcessId: 1, ProjectTaskName: 1 } },
        ],
        as: 'tasks'
      }
    },
    { $unwind: { path: '$tasks', preserveNullAndEmptyArrays: true } },

    // activity
    {
      $lookup: {
        from: 'projectactivity',
        let: { taskId: '$tasks._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectTaskId', '$$taskId'] } } },
          { $project: { ProjectTaskId: 1, ProjectActivityName: 1 } },
        ],
        as: 'activities'
      }
    },
    { $unwind: { path: '$activities', preserveNullAndEmptyArrays: true } },

    // subactivity
    {
      $lookup: {
        from: 'projectsubactivity',
        let: { actId: '$activities._id' },
        pipeline: [
          { $match: { $expr: { $eq: ['$ProjectActivityId', '$$actId'] } } },
          { $project: { ProjectActivityId: 1, ProjectSubActivityName: 1 } },
        ],
        as: 'subactivities'
      }
    },
    { $unwind: { path: '$subactivities', preserveNullAndEmptyArrays: true } },

    // final row
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

  return users.aggregate(pipeline, aggOpts(opts, 'getStagegateHierarchyRows')).toArray();
}


async function getChecklistIdsParallel({ processIds, taskIds, activityIds, subactivityIds }, opts = {}) {
  const db = getDb(opts);

  const cfgs = [
    { collection: 'projectprocesschecklist',     field: 'ProcessId',     ids: processIds,     key: 'byProcess',     hint: HINTS.projectprocesschecklist },
    { collection: 'projecttaskchecklist',        field: 'TaskId',        ids: taskIds,        key: 'byTask',        hint: HINTS.projecttaskchecklist },
    { collection: 'projectactivitychecklist',    field: 'ActivityId',    ids: activityIds,    key: 'byActivity',    hint: HINTS.projectactivitychecklist },
    { collection: 'projectsubactivitychecklist', field: 'SubActivityId', ids: subactivityIds, key: 'bySubactivity', hint: HINTS.projectsubactivitychecklist },
  ];

  const result = { byProcess: Object.create(null), byTask: Object.create(null), byActivity: Object.create(null), bySubactivity: Object.create(null), all: new Set() };

  await Promise.all(cfgs.map(async ({ collection, field, ids, key, hint }) => {
    const list = toObjectIds(ids);
    if (!list.length) return;

    let cursor = db.collection(collection)
      .find({ [field]: { $in: list } }, { projection: { _id: 1, [field]: 1 }, ...findOpts(opts) });

    if (hint) cursor = cursor.hint(hint);

    const rows = await cursor.toArray();

    const bucket = result[key];
    for (const row of rows) {
      const owner = String(row[field]);
      const cid = String(row._id);
      if (!bucket[owner]) bucket[owner] = [];
      bucket[owner].push(cid);
      result.all.add(cid);
    }
  }));

  return result;
}


async function getFilesByChecklistIds(checklistIds = [], opts = {}) {
  if (!Array.isArray(checklistIds) || checklistIds.length === 0) return [];

  const db = getDb(opts);
  const fileCollections = [
    { name: 'projectprocessfiles',     hint: HINTS.projectprocessfiles },
    { name: 'projecttaskfiles',        hint: HINTS.projecttaskfiles },
    { name: 'projectactivityfiles',    hint: HINTS.projectactivityfiles },
    { name: 'projectsubactivityfiles', hint: HINTS.projectsubactivityfiles }
  ];

  const queryIds = toObjectIds(checklistIds);

  const batches = await Promise.all(
    fileCollections.map(async ({ name, hint }) => {
      let cursor = db.collection(name).find(
        { ChecklistId: { $in: queryIds } },
        { projection: { _id: 1, ChecklistId: 1, FileId: 1, FileName: 1, name: 1 }, ...findOpts(opts) }
      );
      if (hint) cursor = cursor.hint(hint);
      return cursor.toArray();
    })
  );

  const out = [];
  for (const list of batches) {
    for (const r of list) {
      const fileName = r.FileName || r.name;
      if (!fileName || !hasAllowedExt(fileName)) continue;

      out.push({
        fileId: String(r.FileId ?? r.fileId ?? r._id), 
        docId: String(r._id),
        checklistId: String(r.ChecklistId),
        fileName
      });
    }
  }
  return out;
}


async function getThreadsByChecklistIds(checklistIdStrs = [], opts = {}) {
  if (!Array.isArray(checklistIdStrs) || checklistIdStrs.length === 0) return [];

  const db = getDb(opts);
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

  return threads.aggregate(pipeline, aggOpts(opts, 'getThreadsByChecklistIds')).toArray();
}


async function getFileContentMetaByIds(fileIds = [], opts = {}) {
  if (!Array.isArray(fileIds) || fileIds.length === 0) return {};

  const objIds = [];
  for (const raw of fileIds) {
    const s = String(raw);
    if (ObjectId.isValid(s)) objIds.push(new ObjectId(s));
  }
  if (!objIds.length) return {};

  const db = getDb(opts);
  const coll = db.collection('filecontent');

  let cursor = coll.find(
    { _id: { $in: objIds } },
    { projection: { FileContent: 1, filecontent: 1, CreatedDate: 1 }, ...findOpts(opts) }
  );
  if (HINTS.filecontent_byId) cursor = cursor.hint(HINTS.filecontent_byId);

  const docs = await cursor.toArray();

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


async function getFileContentMetaByFileNames(fileNames = [], opts = {}) {
  if (!Array.isArray(fileNames) || fileNames.length === 0) return {};

  const names = [...new Set(fileNames.filter(Boolean).map(String))];
  if (!names.length) return {};

  const db = getDb(opts);
  const coll = db.collection('filecontent');

  let cursor = coll.find(
    { FileName: { $in: names } },
    { projection: { FileName: 1, FileContent: 1, filecontent: 1, CreatedDate: 1 }, ...findOpts(opts) }
  );
  if (HINTS.filecontent_byName) cursor = cursor.hint(HINTS.filecontent_byName);

  const docs = await cursor.toArray();

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


async function getUserAssignedOwnerIds(pmwbUserId, opts = {}) {
  const db = getDb(opts);
  const assignee = toInt(pmwbUserId);

  const assignColl = db.collection('raciassigness');

  const assignments = await assignColl.find(
    { $or: [{ assigneeId: assignee }, { AssigneeId: assignee }] },
    { projection: { raciId: 1, RaciId: 1, raciGroup: 1, RaciGroup: 1 }, ...findOpts(opts) }
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
      .find({ _id: { $in: toObjectIds(idsByGroup.process) } }, { projection: { ProcessId: 1 }, ...findOpts(opts) })
      .toArray();
    rows.forEach(r => r?.ProcessId && result.process.add(String(r.ProcessId)));
  }
  if (idsByGroup.task.length) {
    const rows = await db.collection('projecttaskraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.task) } }, { projection: { TaskId: 1 }, ...findOpts(opts) })
      .toArray();
    rows.forEach(r => r?.TaskId && result.task.add(String(r.TaskId)));
  }
  if (idsByGroup.activity.length) {
    const rows = await db.collection('projectactivityraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.activity) } }, { projection: { ActivityId: 1 }, ...findOpts(opts) })
      .toArray();
    rows.forEach(r => r?.ActivityId && result.activity.add(String(r.ActivityId)));
  }
  if (idsByGroup.subactivity.length) {
    const rows = await db.collection('projectsubactivityraci')
      .find({ _id: { $in: toObjectIds(idsByGroup.subactivity) } }, { projection: { SubActivityId: 1 }, ...findOpts(opts) })
      .toArray();
    rows.forEach(r => r?.SubActivityId && result.subactivity.add(String(r.SubActivityId)));
  }

  return result;
}


module.exports = {
  getStagegateHierarchyRows,
  getChecklistIdsParallel,
  getFilesByChecklistIds,
  getThreadsByChecklistIds,
  getFileContentMetaByIds,
  getFileContentMetaByFileNames,
  getUserAssignedOwnerIds,
};
