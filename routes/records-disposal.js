'use strict';

const express = require('express');
const { appendEvent } = require('../migrations/0050_records_module.js');
const { runDisposalCheck } = require('../jobs/disposal-checker.js');

function parseExtendYears(value) {
  if (!value) return 0;
  const m = String(value).match(/extend_(\d+)y/i);
  return m ? parseInt(m[1], 10) : 0;
}

function addYearsIsoDate(isoDate, years) {
  if (!isoDate || !years) return isoDate || null;
  const [y, m, d] = String(isoDate).slice(0, 10).split('-').map((x) => parseInt(x, 10));
  if ([y, m, d].some((n) => Number.isNaN(n))) return isoDate;
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCFullYear(dt.getUTCFullYear() + years);
  return dt.toISOString().slice(0, 10);
}

function createRecordsDisposalRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '1mb' }));
  router.use(requireAuth);

  router.get('/queue', (_req, res) => {
    try {
      const rows = db.prepare(`
        SELECT
          w.*,
          r.record_number,
          r.title AS record_title,
          r.state AS record_state,
          r.classification_id,
          r.trigger_date,
          r.retention_due_date,
          ds.code AS disposal_schedule_code,
          ds.name AS disposal_schedule_name,
          pu.name AS proposed_by_name,
          au.name AS approved_by_name,
          CAST(julianday('now') - julianday(w.deadline) AS INTEGER) AS days_overdue,
          (
            SELECT COUNT(1) FROM rm_components c WHERE c.record_id = r.id
          ) AS components_count
        FROM rm_disposal_workflow w
        JOIN rm_records r ON r.id = w.record_id
        LEFT JOIN rm_disposal_schedules ds ON ds.id = r.disposal_schedule_id
        LEFT JOIN users pu ON pu.id = w.proposed_by
        LEFT JOIN users au ON au.id = w.approved_by
        ORDER BY w.status = 'completed' ASC, w.deadline ASC, w.triggered_at DESC
      `).all();

      const now = new Date();
      const withGroup = rows.map((r) => {
        const deadline = new Date(r.deadline);
        const diffDays = Math.floor((deadline.getTime() - now.getTime()) / 86400000);
        let queue_group = 'pending';
        if (r.status === 'completed') queue_group = 'completed';
        else if (diffDays < 0) queue_group = 'overdue';
        else if (diffDays <= 30) queue_group = 'due_soon';
        return { ...r, queue_group };
      });
      return res.json(withGroup);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không tải được disposal queue' });
    }
  });

  router.post('/:workflowId/propose', (req, res) => {
    try {
      const workflowId = String(req.params.workflowId || '').trim();
      const body = req.body || {};
      const proposedAction = String(body.proposedAction || '').trim();
      const proposalNotes = String(body.proposalNotes || '').trim();
      if (!proposedAction || !proposalNotes) {
        return res.status(400).json({ error: 'proposedAction và proposalNotes là bắt buộc' });
      }
      const wf = db.prepare('SELECT * FROM rm_disposal_workflow WHERE id = ?').get(workflowId);
      if (!wf) return res.status(404).json({ error: 'Workflow không tồn tại' });
      if (wf.status === 'completed') return res.status(400).json({ error: 'Workflow đã hoàn tất' });
      const before = wf;
      db.prepare(`
        UPDATE rm_disposal_workflow
        SET proposed_action = @proposed_action,
            proposed_by = @proposed_by,
            proposed_at = datetime('now'),
            proposal_notes = @proposal_notes
        WHERE id = @id
      `).run({
        id: workflowId,
        proposed_action: proposedAction,
        proposed_by: req.user.id,
        proposal_notes: proposalNotes,
      });
      const after = db.prepare('SELECT * FROM rm_disposal_workflow WHERE id = ?').get(workflowId);
      appendEvent(db, {
        event_type: 'disposal_propose',
        entity_type: 'record',
        entity_id: after.record_id,
        actor_id: req.user.id,
        before_state: before,
        after_state: after,
      });
      return res.json(after);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi propose workflow' });
    }
  });

  router.post('/:workflowId/approve', (req, res) => {
    try {
      const role = req.user && req.user.role;
      if (role !== 'admin' && role !== 'director') {
        return res.status(403).json({ error: 'Chỉ admin hoặc director được phê duyệt' });
      }
      const workflowId = String(req.params.workflowId || '').trim();
      const body = req.body || {};
      const approvedAction = String(body.approvedAction || '').trim();
      const approvalNotes = String(body.approvalNotes || '').trim();
      if (!approvedAction || !approvalNotes) {
        return res.status(400).json({ error: 'approvedAction và approvalNotes là bắt buộc' });
      }

      const wf = db.prepare('SELECT * FROM rm_disposal_workflow WHERE id = ?').get(workflowId);
      if (!wf) return res.status(404).json({ error: 'Workflow không tồn tại' });
      if (wf.status === 'completed') return res.status(400).json({ error: 'Workflow đã completed' });
      const rec = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(wf.record_id);
      if (!rec) return res.status(404).json({ error: 'Record của workflow không tồn tại' });
      const ds = rec.disposal_schedule_id
        ? db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(rec.disposal_schedule_id)
        : null;

      const before = { workflow: wf, record: rec };

      const tx = db.transaction(() => {
        db.prepare(`
          UPDATE rm_disposal_workflow
          SET status = 'completed',
              approved_action = @approved_action,
              approved_by = @approved_by,
              approved_at = datetime('now'),
              approval_notes = @approval_notes,
              completed_at = datetime('now')
          WHERE id = @id
        `).run({
          id: workflowId,
          approved_action: approvedAction,
          approved_by: req.user.id,
          approval_notes: approvalNotes,
        });

        if (approvedAction === 'destruction') {
          db.prepare(`
            UPDATE rm_records
            SET state = 'disposed',
                disposal_action_taken = 'destroyed',
                disposal_confirmed_at = datetime('now'),
                disposal_confirmed_by = @uid,
                updated_at = datetime('now')
            WHERE id = @rid
          `).run({ rid: rec.id, uid: req.user.id });
        } else if (approvedAction === 'preservation') {
          db.prepare(`
            UPDATE rm_records
            SET state = 'preserved',
                disposal_action_taken = 'preserved',
                disposal_confirmed_at = datetime('now'),
                disposal_confirmed_by = @uid,
                updated_at = datetime('now')
            WHERE id = @rid
          `).run({ rid: rec.id, uid: req.user.id });
        } else if (approvedAction === 'extend') {
          const years = parseExtendYears(ds && ds.review_action_retain);
          const newDue = years > 0 ? addYearsIsoDate(rec.retention_due_date || rec.trigger_date, years) : rec.retention_due_date;
          db.prepare(`
            UPDATE rm_records
            SET state = 'active',
                retention_due_date = @due,
                disposal_action_taken = 'extended',
                disposal_alert_sent_at = NULL,
                disposal_confirmed_at = datetime('now'),
                disposal_confirmed_by = @uid,
                updated_at = datetime('now')
            WHERE id = @rid
          `).run({ rid: rec.id, due: newDue, uid: req.user.id });
        } else {
          throw new Error('approvedAction không hợp lệ');
        }
      });
      tx();

      const afterWorkflow = db.prepare('SELECT * FROM rm_disposal_workflow WHERE id = ?').get(workflowId);
      const afterRecord = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(rec.id);
      appendEvent(db, {
        event_type: 'disposal_confirm',
        entity_type: 'record',
        entity_id: rec.id,
        entity_title: rec.title,
        actor_id: req.user.id,
        before_state: before,
        after_state: { workflow: afterWorkflow, record: afterRecord },
      });
      return res.json({ workflow: afterWorkflow, record: afterRecord });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi approve workflow' });
    }
  });

  router.post('/run-check', (req, res) => {
    try {
      const role = req.user && req.user.role;
      if (role !== 'admin') return res.status(403).json({ error: 'Chỉ admin được chạy check thủ công' });
      const result = runDisposalCheck(db);
      return res.json(result);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi chạy disposal check' });
    }
  });

  return router;
}

module.exports = createRecordsDisposalRouter;
