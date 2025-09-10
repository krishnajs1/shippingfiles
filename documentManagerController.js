const docSvc = require('../../services/DocumentManagerService');

const PRUNE_EMPTY = true; // 

function toUiDoc(fileId, fileName) {
  if (!fileName || !fileId) return null;
  return { bucket: String(fileId), displayName: fileName, key: String(fileId) };
}
function pushUniqueDoc(arr, fileId, fileName) {
  const ref = toUiDoc(fileId, fileName);
  if (!ref) return;
  if (!arr.some(d => d.key === ref.key)) arr.push(ref);
}

class DocumentManagerController {
  async getDocumentsByUser(req, res) {
    try {
      const pmwbUserId = req.params.userId;
      const rows = await docSvc.getStagegateHierarchyRows(pmwbUserId, {});

      const tree = {};
      const phaseIdOf = (name) => String(name || 'Site Wide').toLowerCase().replace(/\s+/g, '-');

      const phaseOwners = new Map();
      const ownerNames = { process: new Map(), task: new Map(), activity: new Map(), subactivity: new Map() };

      for (const r of rows) {
        const community = r.community || r.projectName || 'Community';
        const phaseName = r.phaseName || 'Site Wide';
        const phaseId = phaseIdOf(phaseName);
        const phaseKey = `${community}::${phaseId}`;

        if (!tree[community]) tree[community] = [];
        if (!tree[community].some(p => p.id === phaseId)) {
          tree[community].push({ id: phaseId, name: phaseName, children: [], fileCount: 0, _fileCount: 0 });
        }

        if (!phaseOwners.has(phaseKey)) {
          phaseOwners.set(phaseKey, { procIds: new Set(), taskIds: new Set(), actIds: new Set(), subIds: new Set() });
        }
        const own = phaseOwners.get(phaseKey);

        if (r.processId)    { const id = String(r.processId);    own.procIds.add(id); ownerNames.process.set(id, r.processName || null); }
        if (r.taskId)       { const id = String(r.taskId);       own.taskIds.add(id); ownerNames.task.set(id, r.taskName || null); }
        if (r.activityId)   { const id = String(r.activityId);   own.actIds.add(id);  ownerNames.activity.set(id, r.activityName || null); }
        if (r.subactivityId){ const id = String(r.subactivityId);own.subIds.add(id);  ownerNames.subactivity.set(id, r.subactivityName || null); }
      }

      const allProc = new Set(), allTask = new Set(), allAct = new Set(), allSub = new Set();
      for (const v of phaseOwners.values()) {
        v.procIds.forEach(id => allProc.add(id));
        v.taskIds.forEach(id => allTask.add(id));
        v.actIds.forEach(id => allAct.add(id));
        v.subIds.forEach(id => allSub.add(id));
      }

      const { byProcess, byTask, byActivity, bySubactivity, all } =
        await docSvc.getChecklistIdsParallel({
          processIds: [...allProc],
          taskIds: [...allTask],
          activityIds: [...allAct],
          subactivityIds: [...allSub],
        });

      const fileRows = await docSvc.getFilesByChecklistIds([...all]);
      const filesByChecklist = {};
      for (const f of fileRows) {
        const cid = String(f.checklistId);
        (filesByChecklist[cid] ||= []);
        if (!filesByChecklist[cid].some(x => x.fileId === f.fileId)) {
          filesByChecklist[cid].push({ fileId: String(f.fileId), fileName: f.fileName });
        }
      }

      // Build a single node (process/task/activity/subactivity), tagged with its kind
      const buildNode = (id, name, cids = [], kind) => {
        const node = {
          id: String(id),
          name: name ?? String(id),               
          checklistId: null,
          checklistIdStr: null,
          documents: { checklist: [], general: [], final: [] },
          fileCount: 0,                             // public count
          _fileCount: 0,                            // internal (used for sort/prune)
          _kind: kind                               // internal discriminator for sorting
        };

        // unique checklist ids
        const uniq = [];
        const seen = new Set();
        for (const raw of (Array.isArray(cids) ? cids : [])) {
          const cid = String(raw);
          if (seen.has(cid)) continue;
          seen.add(cid);
          uniq.push(cid);
        }

        if (uniq.length) node.checklistId = node.checklistIdStr = uniq[0];

        // gather checklist docs
        for (const cid of uniq) {
          for (const it of (filesByChecklist[cid] || [])) {
            pushUniqueDoc(node.documents.checklist, it.fileId, it.fileName);
          }
        }

        // dedupe into final[]
        const finalMap = new Map();
        for (const d of node.documents.checklist) finalMap.set(d.key, d);
        node.documents.final = [...finalMap.values()];

        node._fileCount = node.documents.final.length;
        node.fileCount = node._fileCount;          // expose clean count

        return node;
      };

      // Build phases →children and compute counts
      for (const [community, phases] of Object.entries(tree)) {
        for (const phase of phases) {
          const key = `${community}::${phase.id}`;
          const own = phaseOwners.get(key);

          let children = [];
          if (own) {
            for (const pid of own.procIds) children.push(buildNode(pid, ownerNames.process.get(pid), byProcess[pid] || [], 'process'));
            for (const tid of own.taskIds) children.push(buildNode(tid, ownerNames.task.get(tid), byTask[tid] || [], 'task'));
            for (const aid of own.actIds)  children.push(buildNode(aid, ownerNames.activity.get(aid), byActivity[aid] || [], 'activity'));
            for (const sid of own.subIds)  children.push(buildNode(sid, ownerNames.subactivity.get(sid), bySubactivity[sid] || [], 'subactivity'));
          }

          // prune empty nodes if enabled
          if (PRUNE_EMPTY) {
            children = children.filter(n => (n._fileCount || 0) > 0);
          }

          // sort children by kind first (process ,task, activity, subactivity), then by name
          const rank = { process: 1, task: 2, activity: 3, subactivity: 4 };
          children.sort((a, b) => {
            const r = (rank[a._kind] || 99) - (rank[b._kind] || 99);
            if (r !== 0) return r;
            return String(a.name).localeCompare(String(b.name));
          });

          // phase counts
          const phaseFileCount = children.reduce((acc, n) => acc + (n._fileCount || 0), 0);
          phase.children = children;
          phase._fileCount = phaseFileCount;   // internal for sort/prune
          phase.fileCount = phaseFileCount;    // public for UI
        }

        // prune empty phases if enabled
        if (PRUNE_EMPTY) {
          tree[community] = phases.filter(p => (p._fileCount || 0) > 0);
        }

        // sort phases: fileCount desc, name asc 
        tree[community].sort((a, b) => {
          const df = (b._fileCount || 0) - (a._fileCount || 0);
          return df !== 0 ? df : String(a.name).localeCompare(String(b.name));
        });
      }

 
      for (const phases of Object.values(tree)) {
        for (const phase of phases) {
          delete phase._fileCount;
          // for (const child of (phase.children || [])) {
          //   // delete child._fileCount;
          //   // delete child._kind; //
          // }
        }
      }

      // prune empty communities if enabled
      if (PRUNE_EMPTY) {
        for (const c of Object.keys(tree)) if (!tree[c].length) delete tree[c];
      }

      res.status(200).setHeader('X-XSS-Protection', '1; mode=block').json({ tree });
    } catch (err) {
      console.error('Error fetching Documents of a User (phase-level):', err);
      res.status(500).json({ message: 'Error fetching Documents of a User', error: err.message });
    }
  }

 
async createFileComment(req, res) {
  const { fileId, text, page, anchorText, rect, userId, createdBy ,yPct ,xPct } = req.body || {};
  
  const effectiveUserId = req.user?._id || userId;
  const payload = { fileId, text, page, anchorText, rect ,yPct ,xPct , userId: effectiveUserId, createdBy: createdBy || effectiveUserId };
  const created = await docSvc.insertFileComment(payload);       
  const rows = await docSvc.getFileCommentsByFileId(fileId);         
  const full = rows.find(r => String(r._id) === String(created._id)) || created;
  res.status(201).json(full);
}

  async getFileData2(req, res) {
    try {
      const { key } = req.body || {};
      const fileData = await docSvc.getFileContentMetaByIds([key]);
      res.status(200).setHeader('X-XSS-Protection', '1; mode=block').json({ fileData });
    } catch (err) {
      console.error('Error fetching Document metadata of a File:', err);
      res.status(500).json({ message: 'Error fetching Document Meta Data', error: err.message });
    }
  }


  async getFileData(req, res) {
  try {
  
    const keySingle = req.body?.key;
    const keysArray = Array.isArray(req.body?.keys) ? req.body.keys : [];
    const keys = keysArray.length ? keysArray : (keySingle ? [keySingle] : []);

    if (!keys.length) {
      return res.status(400)
        .setHeader('X-XSS-Protection', '1; mode=block')
        .json({ message: 'Provide key or keys[] in body' });
    }


    const fileDataRaw = await docSvc.getFileContentMetaByIds(keys);

    let comments=[]

    const fileData = {};
    for (const k of keys) {
      const meta = fileDataRaw?.[k] || {}; 

     const eachFileComments= await docSvc.getFileCommentsByFileId(k);  
     if(eachFileComments.length) 
     comments=[...comments,...eachFileComments]
     

      // ---- DUMMY versions list (array) ---- ,later i need to fetch or kiran paul db design fetchinhig linked versions file and related data
      const versions = [
        {
          version: 3,
          bucket: 'my-prod-bucket',
          displayName: 'sample_v3.xlsx',
          key: 'docs/sample_v3.xlsx',
          url: 'https://example.com/docs/sample_v3.xlsx',
          createdAt: new Date('2025-09-06T10:05:00-05:00')
        },
        {
          version: 2,
          bucket: 'my-prod-bucket',
          displayName: 'sample_v2.xlsx',
          key: 'docs/sample_v2.xlsx',
          url: 'https://example.com/docs/sample_v2.xlsx',
          createdAt: new Date('2025-09-01T12:00:00-05:00')
        },
        {
          version: 1,
          bucket: 'my-prod-bucket',
          displayName: 'sample_v1.xlsx',
          key: 'docs/sample_v1.xlsx',
          url: 'https://example.com/docs/sample_v1.xlsx',
          createdAt: new Date('2025-08-25T09:30:00-05:00')
        }
      ];

   

      fileData[k] = {
        ...meta,               
        url: meta?.url ?? null, 
        comments,
        versions  
      };

    
    }

    return res
      .status(200)
      .setHeader('X-XSS-Protection', '1; mode=block')
      .json({ fileData });
  } catch (err) {
    console.error('Error fetching Document metadata of a File:', err);
    res
      .status(500)
      .json({ message: 'Error fetching Document Meta Data', error: err.message });
  }
}

}



module.exports = new DocumentManagerController();
