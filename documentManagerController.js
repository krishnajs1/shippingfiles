const docSvc = require('../../services/DocumentManagerService');

const PRUNE_EMPTY = true; 


const s = (v) => (v == null ? '' : String(v));
const sId = (v) => s(v);
const norm = (v) => s(v).trim();
const phaseSlug = (name) => norm(name || 'Site Wide').toLowerCase().replace(/\s+/g, '-');


function toUiDoc(fileId, fileName) {
  if (!fileName || !fileId) return null;
  const key = s(fileId);
  return { bucket: key, displayName: fileName, key };
}
function pushUniqueDoc(arr, fileId, fileName, seenSet) {
  const key = s(fileId);
  if (seenSet.has(key)) return;
  const ref = toUiDoc(key, fileName);
  if (!ref) return;
  seenSet.add(key);
  arr.push(ref);
}


class DocumentManagerController {
  async getDocumentsByUser(req, res) {
    try {
      const pmwbUserId = req.params.userId;


      const rows = await docSvc.getStagegateHierarchyRows(pmwbUserId, {});
      if (!rows || rows.length === 0) {
        return res
          .status(200)
          .setHeader('X-XSS-Protection', '1; mode=block')
          .json({ tree: {} });
      }

     
      const tree = Object.create(null);      
      const phaseIndex = new Map();          
      const phaseOwners = new Map();          

      const ownerNames = {
        process: new Map(),
        task: new Map(),
        activity: new Map(),
        subactivity: new Map(),
      };

      for (const r of rows) {
        const community = norm(r.community || r.projectName || 'Community');
        const phaseName = norm(r.phaseName || 'Site Wide');
        const pid = phaseSlug(phaseName);
        const pkey = `${community}::${pid}`;

        let phases = tree[community];
        if (!phases) phases = tree[community] = [];

        let phaseNode = phaseIndex.get(pkey);
        if (!phaseNode) {
          phaseNode = { id: pid, name: phaseName, children: [], fileCount: 0, _fileCount: 0 };
          phaseIndex.set(pkey, phaseNode);
          phases.push(phaseNode);
        }

        let owners = phaseOwners.get(pkey);
        if (!owners) {
          owners = { procIds: new Set(), taskIds: new Set(), actIds: new Set(), subIds: new Set() };
          phaseOwners.set(pkey, owners);
        }

        if (r.processId) {
          const id = sId(r.processId);
          owners.procIds.add(id);
          if (!ownerNames.process.has(id)) ownerNames.process.set(id, r.processName || null);
        }
        if (r.taskId) {
          const id = sId(r.taskId);
          owners.taskIds.add(id);
          if (!ownerNames.task.has(id)) ownerNames.task.set(id, r.taskName || null);
        }
        if (r.activityId) {
          const id = sId(r.activityId);
          owners.actIds.add(id);
          if (!ownerNames.activity.has(id)) ownerNames.activity.set(id, r.activityName || null);
        }
        if (r.subactivityId) {
          const id = sId(r.subactivityId);
          owners.subIds.add(id);
          if (!ownerNames.subactivity.has(id)) ownerNames.subactivity.set(id, r.subactivityName || null);
        }
      }

      // 3) Consolidate all unique owner ids
      const allProc = new Set(), allTask = new Set(), allAct = new Set(), allSub = new Set();
      for (const v of phaseOwners.values()) {
        v.procIds.forEach(id => allProc.add(id));
        v.taskIds.forEach(id => allTask.add(id));
        v.actIds.forEach(id => allAct.add(id));
        v.subIds.forEach(id => allSub.add(id));
      }

      const { byProcess, byTask, byActivity, bySubactivity, all } =
        await docSvc.getChecklistIdsParallel({
          processIds: allProc.size ? [...allProc] : [],
          taskIds: allTask.size ? [...allTask] : [],
          activityIds: allAct.size ? [...allAct] : [],
          subactivityIds: allSub.size ? [...allSub] : [],
        });

      // 4) Files for all checklist ids
      const fileRows = all.size ? await docSvc.getFilesByChecklistIds([...all]) : [];
      const filesByChecklist = Object.create(null);
      for (const f of fileRows) {
        const cid = sId(f.checklistId);
        let arr = filesByChecklist[cid];
        if (!arr) arr = filesByChecklist[cid] = [];
        if (!arr.some(x => x.fileId === s(f.fileId))) {
          arr.push({ fileId: s(f.fileId), fileName: f.fileName });
        }
      }

      const buildNode = (id, name, cids = [], kind) => {
        const node = {
          id: sId(id),
          name: name ?? sId(id),
          checklistId: null,
          checklistIdStr: null,
          documents: { checklist: [], general: [], final: [] },
          fileCount: 0,
          _fileCount: 0,
          _kind: kind
        };

        const seenCids = new Set();
        for (const raw of (Array.isArray(cids) ? cids : [])) {
          const cid = sId(raw);
          if (cid) seenCids.add(cid);
        }

        if (seenCids.size) {
          const first = seenCids.values().next().value;
          node.checklistId = first;
          node.checklistIdStr = first;
        }

        const seenFileIds = new Set();
        for (const cid of seenCids) {
          const list = filesByChecklist[cid];
          if (!list || list.length === 0) continue;
          for (const it of list) pushUniqueDoc(node.documents.checklist, it.fileId, it.fileName, seenFileIds);
        }

        node.documents.final = node.documents.checklist.slice(0);
        node._fileCount = node.documents.final.length;
        node.fileCount = node._fileCount;

        return node;
      };

      const rank = Object.freeze({ process: 1, task: 2, activity: 3, subactivity: 4 });

      for (const [pkey, owners] of phaseOwners.entries()) {
        const phase = phaseIndex.get(pkey);
        if (!phase) continue;

        const kids = phase.children;

        for (const pid of owners.procIds) {
          kids.push(buildNode(pid, ownerNames.process.get(pid), byProcess[pid] || [], 'process'));
        }
        for (const tid of owners.taskIds) {
          kids.push(buildNode(tid, ownerNames.task.get(tid), byTask[tid] || [], 'task'));
        }
        for (const aid of owners.actIds) {
          kids.push(buildNode(aid, ownerNames.activity.get(aid), byActivity[aid] || [], 'activity'));
        }
        for (const sid of owners.subIds) {
          kids.push(buildNode(sid, ownerNames.subactivity.get(sid), bySubactivity[sid] || [], 'subactivity'));
        }

        if (PRUNE_EMPTY) {
          phase.children = kids.filter(n => (n._fileCount || 0) > 0);
        }

        phase.children.sort((a, b) => {
          const r = (rank[a._kind] || 99) - (rank[b._kind] || 99);
          if (r !== 0) return r;
          return s(a.name).localeCompare(s(b.name));
        });

        let phaseCount = 0;
        for (let i = 0; i < phase.children.length; i++) phaseCount += (phase.children[i]._fileCount || 0);
        phase._fileCount = phaseCount;
        phase.fileCount = phaseCount;
      }

      for (const community of Object.keys(tree)) {
        let phases = tree[community];
        if (!Array.isArray(phases)) continue;

        if (PRUNE_EMPTY) phases = tree[community] = phases.filter(p => (p._fileCount || 0) > 0);

        phases.sort((a, b) => {
          const df = (b._fileCount || 0) - (a._fileCount || 0);
          return df !== 0 ? df : s(a.name).localeCompare(s(b.name));
        });
      }
      if (PRUNE_EMPTY) for (const c of Object.keys(tree)) if (!tree[c].length) delete tree[c];

      for (const phases of Object.values(tree)) {
        for (const phase of phases) delete phase._fileCount;
      }

      res.status(200).setHeader('X-XSS-Protection', '1; mode=block').json({ tree });
    } catch (err) {
      console.error('Error fetching Documents of a User (phase-level):', err);
      res.status(500).json({ message: 'Error fetching Documents of a User', error: err.message });
    }
  }

  async getFileData(req, res) {
    try {
      const { key } = req.body || {};
      const fileData = await docSvc.getFileContentMetaByIds([key]);
      res.status(200).setHeader('X-XSS-Protection', '1; mode=block').json({ fileData });
    } catch (err) {
      console.error('Error fetching Document metadata of a File:', err);
      res.status(500).json({ message: 'Error fetching Document Meta Data', error: err.message });
    }
  }
}

module.exports = new DocumentManagerController();
