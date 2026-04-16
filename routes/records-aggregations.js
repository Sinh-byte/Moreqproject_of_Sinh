'use strict';

const crypto = require('crypto');
const express = require('express');
const { appendEvent } = require('../migrations/0050_records_module.js');

function generateAggregationCode(db) {
  const year = new Date().getFullYear();
  const prefix = `AGG-${year}-`;
  const rows = db
    .prepare('SELECT code FROM rm_aggregations WHERE code LIKE ?')
    .all(`${prefix}%`);
  let maxSeq = 0;
  const re = new RegExp(`^AGG-${year}-(\\d+)$`);
  for (const r of rows) {
    const m = String(r.code || '').match(re);
    if (!m) continue;
    const n = parseInt(m[1], 10);
    if (!Number.isNaN(n)) maxSeq = Math.max(maxSeq, n);
  }
  return `${prefix}${String(maxSeq + 1).padStart(3, '0')}`;
}

function createRecordsAggregationsRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '1mb' }));
  router.use(requireAuth);

  router.get('/', (req, res) => {
    try {
      const type = req.query.type ? String(req.query.type).trim() : '';
      const where = type ? 'WHERE a.agg_type = @type' : '';
      const rows = db
        .prepare(
          `
        SELECT
          a.*,
          (SELECT COUNT(1) FROM rm_records r WHERE r.aggregation_id = a.id) AS record_count,
          (SELECT COUNT(1) FROM rm_aggregations c WHERE c.parent_id = a.id) AS children_count,
          rp.title AS stims_project_title,
          u.name AS manager_name
        FROM rm_aggregations a
        LEFT JOIN research_projects rp ON rp.id = a.stims_project_id
        LEFT JOIN users u ON u.id = a.manager_id
        ${where}
        ORDER BY a.code
      `
        )
        .all(type ? { type } : {});
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải aggregations' });
    }
  });

  router.get('/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const agg = db
        .prepare(
          `
        SELECT
          a.*,
          rp.title AS stims_project_title,
          u.name AS manager_name
        FROM rm_aggregations a
        LEFT JOIN research_projects rp ON rp.id = a.stims_project_id
        LEFT JOIN users u ON u.id = a.manager_id
        WHERE a.id = ?
      `
        )
        .get(id);
      if (!agg) return res.status(404).json({ error: 'Aggregation không tồn tại' });

      const children = db
        .prepare(
          `
        SELECT a.*, (SELECT COUNT(1) FROM rm_records r WHERE r.aggregation_id = a.id) AS record_count
        FROM rm_aggregations a
        WHERE a.parent_id = ?
        ORDER BY a.code
      `
        )
        .all(id);

      const records = db
        .prepare(
          `
        SELECT id, record_number, title, doc_type, issue_date, state, capture_date
        FROM rm_records
        WHERE aggregation_id = ?
        ORDER BY capture_date DESC
        LIMIT 20
      `
        )
        .all(id);

      const progressRaw = db
        .prepare(
          `
        SELECT doc_type, COUNT(1) AS c
        FROM rm_records
        WHERE aggregation_id = ?
        GROUP BY doc_type
      `
        )
        .all(id);
      const progress = {
        dang_ky: 0,
        tien_do: 0,
        tai_chinh: 0,
        nghiem_thu: 0,
      };
      for (const p of progressRaw) {
        if (p.doc_type === 'plan' || p.doc_type === 'dispatch') progress.dang_ky += p.c;
        else if (p.doc_type === 'memo' || p.doc_type === 'minutes') progress.tien_do += p.c;
        else if (p.doc_type === 'contract') progress.tai_chinh += p.c;
        else if (p.doc_type === 'decision' || p.doc_type === 'regulation') progress.nghiem_thu += p.c;
      }

      const projectExists = agg.stims_project_id
        ? !!db.prepare('SELECT id FROM research_projects WHERE id = ?').get(agg.stims_project_id)
        : true;

      return res.json({
        ...agg,
        children,
        records,
        progress,
        stims_links: {
          stims_project_id: agg.stims_project_id,
          stims_project_title: agg.stims_project_title,
          missing_project_warning: agg.stims_project_id && !projectExists,
        },
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải chi tiết aggregation' });
    }
  });

  router.post('/', (req, res) => {
    try {
      const b = req.body || {};
      const id = crypto.randomUUID();
      const title = String(b.title || '').trim();
      const agg_type = String(b.agg_type || '').trim();
      if (!title || !agg_type) {
        return res.status(400).json({ error: 'title và agg_type là bắt buộc' });
      }
      const row = {
        id,
        code: generateAggregationCode(db),
        title,
        agg_type,
        parent_id: b.parent_id ? String(b.parent_id).trim() : null,
        classification_id: b.classification_id ? String(b.classification_id).trim() : null,
        disposal_schedule_id: b.disposal_schedule_id ? String(b.disposal_schedule_id).trim() : null,
        is_open: b.is_open === 0 || b.is_open === false ? 0 : 1,
        manager_id: b.manager_id ? String(b.manager_id).trim() : null,
        stims_project_id: b.stims_project_id ? String(b.stims_project_id).trim() : null,
        notes: b.notes != null ? String(b.notes) : null,
        created_by: req.user.id,
      };
      db.prepare(`
        INSERT INTO rm_aggregations (
          id, code, title, agg_type, parent_id, classification_id, disposal_schedule_id,
          is_open, manager_id, stims_project_id, notes, created_by, created_at, updated_at
        ) VALUES (
          @id, @code, @title, @agg_type, @parent_id, @classification_id, @disposal_schedule_id,
          @is_open, @manager_id, @stims_project_id, @notes, @created_by, datetime('now'), datetime('now')
        )
      `).run(row);
      const created = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      return res.status(201).json(created);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tạo aggregation' });
    }
  });

  router.patch('/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const old = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      if (!old) return res.status(404).json({ error: 'Aggregation không tồn tại' });
      const b = req.body || {};
      const next = {
        title: Object.prototype.hasOwnProperty.call(b, 'title') ? String(b.title || '').trim() : old.title,
        agg_type: Object.prototype.hasOwnProperty.call(b, 'agg_type') ? String(b.agg_type || '').trim() : old.agg_type,
        parent_id: Object.prototype.hasOwnProperty.call(b, 'parent_id') ? (b.parent_id ? String(b.parent_id).trim() : null) : old.parent_id,
        classification_id: Object.prototype.hasOwnProperty.call(b, 'classification_id') ? (b.classification_id ? String(b.classification_id).trim() : null) : old.classification_id,
        disposal_schedule_id: Object.prototype.hasOwnProperty.call(b, 'disposal_schedule_id') ? (b.disposal_schedule_id ? String(b.disposal_schedule_id).trim() : null) : old.disposal_schedule_id,
        is_open: Object.prototype.hasOwnProperty.call(b, 'is_open') ? (b.is_open ? 1 : 0) : old.is_open,
        manager_id: Object.prototype.hasOwnProperty.call(b, 'manager_id') ? (b.manager_id ? String(b.manager_id).trim() : null) : old.manager_id,
        stims_project_id: Object.prototype.hasOwnProperty.call(b, 'stims_project_id') ? (b.stims_project_id ? String(b.stims_project_id).trim() : null) : old.stims_project_id,
        notes: Object.prototype.hasOwnProperty.call(b, 'notes') ? (b.notes != null ? String(b.notes) : null) : old.notes,
      };
      if (!next.title || !next.agg_type) return res.status(400).json({ error: 'title và agg_type là bắt buộc' });
      db.prepare(`
        UPDATE rm_aggregations
        SET title=@title, agg_type=@agg_type, parent_id=@parent_id, classification_id=@classification_id,
            disposal_schedule_id=@disposal_schedule_id, is_open=@is_open, manager_id=@manager_id,
            stims_project_id=@stims_project_id, notes=@notes, updated_at=datetime('now')
        WHERE id=@id
      `).run({ ...next, id });
      const updated = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi cập nhật aggregation' });
    }
  });

  router.patch('/:id/close', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const old = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      if (!old) return res.status(404).json({ error: 'Aggregation không tồn tại' });
      db.prepare('UPDATE rm_aggregations SET is_open = 0, updated_at = datetime(\'now\') WHERE id = ?').run(id);
      const updated = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'aggregation',
        entity_id: id,
        entity_title: updated.title,
        actor_id: req.user.id,
        before_state: old,
        after_state: updated,
        payload: { action: 'close' },
      });
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi đóng aggregation' });
    }
  });

  router.post('/:id/add-record', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const recordId = String((req.body && req.body.recordId) || '').trim();
      if (!recordId) return res.status(400).json({ error: 'recordId là bắt buộc' });
      const agg = db.prepare('SELECT * FROM rm_aggregations WHERE id = ?').get(id);
      if (!agg) return res.status(404).json({ error: 'Aggregation không tồn tại' });
      if (agg.is_open !== 1) {
        return res.status(400).json({ error: 'Aggregation đã đóng, không thể thêm record mới' });
      }
      const rec = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(recordId);
      if (!rec) return res.status(404).json({ error: 'Record không tồn tại' });
      db.prepare('UPDATE rm_records SET aggregation_id = ?, updated_at = datetime(\'now\') WHERE id = ?')
        .run(id, recordId);
      const updated = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(recordId);
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi gán record vào aggregation' });
    }
  });

  return router;
}

module.exports = createRecordsAggregationsRouter;
