'use strict';

const crypto = require('crypto');
const express = require('express');
const { appendEvent } = require('../migrations/0050_records_module.js');

function addYearsIsoDate(isoDate, years) {
  if (!isoDate || years == null || Number.isNaN(years)) return null;
  const parts = String(isoDate).slice(0, 10).split('-');
  if (parts.length !== 3) return null;
  const y = parseInt(parts[0], 10);
  const m = parseInt(parts[1], 10);
  const d = parseInt(parts[2], 10);
  if ([y, m, d].some((n) => Number.isNaN(n))) return null;
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCFullYear(dt.getUTCFullYear() + years);
  return dt.toISOString().slice(0, 10);
}

function createRecordsAdminRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '2mb' }));
  router.use(requireAuth);
  router.use((req, res, next) => {
    const role = req.user && req.user.role;
    if (role !== 'admin' && role !== 'records_manager') {
      return res.status(403).json({ error: 'Không có quyền admin records' });
    }
    return next();
  });

  // CLASSIFICATIONS
  router.get('/classifications', (_req, res) => {
    try {
      const rows = db.prepare(`
        SELECT
          c.*,
          d.code AS disposal_schedule_code,
          d.name AS disposal_schedule_name,
          (SELECT COUNT(1) FROM rm_records r WHERE r.classification_id = c.id) AS record_count
        FROM rm_classifications c
        LEFT JOIN rm_disposal_schedules d ON d.id = c.disposal_schedule_id
        ORDER BY c.sort_order, c.code
      `).all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải classifications' });
    }
  });

  router.post('/classifications', (req, res) => {
    try {
      const body = req.body || {};
      const id = crypto.randomUUID();
      const code = String(body.code || '').trim();
      const name = String(body.name || '').trim();
      const parentId = body.parent_id ? String(body.parent_id).trim() : null;
      const dsId = body.disposal_schedule_id ? String(body.disposal_schedule_id).trim() : null;
      const description = body.description != null ? String(body.description) : null;
      const inheritDisposal = body.inherit_disposal === 0 || body.inherit_disposal === false ? 0 : 1;
      const isOpen = body.is_open === 0 || body.is_open === false ? 0 : 1;
      const sortOrder = Number.isInteger(body.sort_order) ? body.sort_order : 0;
      if (!code || !name) {
        return res.status(400).json({ error: 'code và name là bắt buộc' });
      }
      if (parentId) {
        const parent = db.prepare('SELECT id FROM rm_classifications WHERE id = ?').get(parentId);
        if (!parent) return res.status(400).json({ error: 'parent_id không tồn tại' });
      }
      if (dsId) {
        const ds = db.prepare('SELECT id FROM rm_disposal_schedules WHERE id = ?').get(dsId);
        if (!ds) return res.status(400).json({ error: 'disposal_schedule_id không tồn tại' });
      }
      db.prepare(`
        INSERT INTO rm_classifications (
          id, code, name, description, parent_id, disposal_schedule_id,
          inherit_disposal, is_open, sort_order, created_by, created_at, updated_at
        ) VALUES (
          @id, @code, @name, @description, @parent_id, @disposal_schedule_id,
          @inherit_disposal, @is_open, @sort_order, @created_by, datetime('now'), datetime('now')
        )
      `).run({
        id,
        code,
        name,
        description,
        parent_id: parentId,
        disposal_schedule_id: dsId,
        inherit_disposal: inheritDisposal,
        is_open: isOpen,
        sort_order: sortOrder,
        created_by: req.user.id,
      });
      const row = db.prepare('SELECT * FROM rm_classifications WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'classification',
        entity_id: id,
        entity_title: name,
        actor_id: req.user.id,
        after_state: row,
      });
      return res.status(201).json(row);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tạo classification' });
    }
  });

  router.patch('/classifications/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const row = db.prepare('SELECT * FROM rm_classifications WHERE id = ?').get(id);
      if (!row) return res.status(404).json({ error: 'Classification không tồn tại' });
      const body = req.body || {};
      const next = {
        name: Object.prototype.hasOwnProperty.call(body, 'name') ? String(body.name || '').trim() : row.name,
        description: Object.prototype.hasOwnProperty.call(body, 'description') ? (body.description != null ? String(body.description) : null) : row.description,
        disposal_schedule_id: Object.prototype.hasOwnProperty.call(body, 'disposal_schedule_id')
          ? (body.disposal_schedule_id ? String(body.disposal_schedule_id).trim() : null)
          : row.disposal_schedule_id,
        is_open: Object.prototype.hasOwnProperty.call(body, 'is_open')
          ? (body.is_open === 0 || body.is_open === false ? 0 : 1)
          : row.is_open,
        inherit_disposal: Object.prototype.hasOwnProperty.call(body, 'inherit_disposal')
          ? (body.inherit_disposal === 0 || body.inherit_disposal === false ? 0 : 1)
          : row.inherit_disposal,
      };
      if (!next.name) return res.status(400).json({ error: 'name không được rỗng' });
      if (next.disposal_schedule_id) {
        const ds = db.prepare('SELECT id FROM rm_disposal_schedules WHERE id = ?').get(next.disposal_schedule_id);
        if (!ds) return res.status(400).json({ error: 'disposal_schedule_id không tồn tại' });
      }
      db.prepare(`
        UPDATE rm_classifications
        SET name = @name,
            description = @description,
            disposal_schedule_id = @disposal_schedule_id,
            is_open = @is_open,
            inherit_disposal = @inherit_disposal,
            updated_at = datetime('now')
        WHERE id = @id
      `).run({ ...next, id });
      const updated = db.prepare('SELECT * FROM rm_classifications WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'classification',
        entity_id: id,
        entity_title: updated.name,
        actor_id: req.user.id,
        before_state: row,
        after_state: updated,
      });
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi cập nhật classification' });
    }
  });

  router.delete('/classifications/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const row = db.prepare('SELECT * FROM rm_classifications WHERE id = ?').get(id);
      if (!row) return res.status(404).json({ error: 'Classification không tồn tại' });
      const childCount = db.prepare('SELECT COUNT(1) AS c FROM rm_classifications WHERE parent_id = ?').get(id).c;
      const recordCount = db.prepare('SELECT COUNT(1) AS c FROM rm_records WHERE classification_id = ?').get(id).c;
      if (childCount > 0 || recordCount > 0) {
        return res.status(400).json({
          error: 'Không thể xóa classification đang có node con hoặc records',
          childCount,
          recordCount,
        });
      }
      db.prepare('DELETE FROM rm_classifications WHERE id = ?').run(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'classification',
        entity_id: id,
        entity_title: row.name,
        actor_id: req.user.id,
        before_state: row,
        payload: { action: 'delete' },
      });
      return res.json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi xóa classification' });
    }
  });

  // DISPOSAL SCHEDULES
  router.get('/disposal-schedules', (_req, res) => {
    try {
      const rows = db.prepare(`
        SELECT
          d.*,
          (SELECT COUNT(1) FROM rm_records r WHERE r.disposal_schedule_id = d.id) AS record_count,
          (SELECT COUNT(1) FROM rm_records r WHERE r.disposal_schedule_id = d.id AND r.state = 'active') AS active_record_count
        FROM rm_disposal_schedules d
        ORDER BY d.code
      `).all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải disposal schedules' });
    }
  });

  router.post('/disposal-schedules', (req, res) => {
    try {
      const b = req.body || {};
      const id = crypto.randomUUID();
      const row = {
        id,
        code: String(b.code || '').trim(),
        name: String(b.name || '').trim(),
        legal_basis: b.legal_basis != null ? String(b.legal_basis) : null,
        trigger_event: String(b.trigger_event || '').trim(),
        retention_years: b.retention_years != null && b.retention_years !== '' ? parseInt(b.retention_years, 10) : null,
        is_permanent: b.is_permanent ? 1 : 0,
        disposal_action: String(b.disposal_action || '').trim(),
        review_action_approve: b.review_action_approve ? String(b.review_action_approve) : null,
        review_action_retain: b.review_action_retain ? String(b.review_action_retain) : null,
        confirmation_days: b.confirmation_days != null ? parseInt(b.confirmation_days, 10) : 30,
        alert_recipients: b.alert_recipients != null ? String(b.alert_recipients) : null,
        notes: b.notes != null ? String(b.notes) : null,
        is_active: b.is_active === 0 || b.is_active === false ? 0 : 1,
        created_by: req.user.id,
      };
      if (!row.code || !row.name || !row.trigger_event || !row.disposal_action) {
        return res.status(400).json({ error: 'Thiếu field bắt buộc: code, name, trigger_event, disposal_action' });
      }
      db.prepare(`
        INSERT INTO rm_disposal_schedules (
          id, code, name, legal_basis, trigger_event, retention_years, is_permanent,
          disposal_action, review_action_approve, review_action_retain, confirmation_days,
          alert_recipients, notes, is_active, created_by, created_at, updated_at
        ) VALUES (
          @id, @code, @name, @legal_basis, @trigger_event, @retention_years, @is_permanent,
          @disposal_action, @review_action_approve, @review_action_retain, @confirmation_days,
          @alert_recipients, @notes, @is_active, @created_by, datetime('now'), datetime('now')
        )
      `).run(row);
      const created = db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'disposal_schedule',
        entity_id: id,
        entity_title: created.name,
        actor_id: req.user.id,
        after_state: created,
      });
      return res.status(201).json(created);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tạo disposal schedule' });
    }
  });

  router.patch('/disposal-schedules/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const old = db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(id);
      if (!old) return res.status(404).json({ error: 'Disposal schedule không tồn tại' });
      const activeUsers = db.prepare(`
        SELECT id, record_number, title, state
        FROM rm_records
        WHERE disposal_schedule_id = ? AND state = 'active'
        ORDER BY capture_date DESC
      `).all(id);

      const b = req.body || {};
      const next = {
        code: Object.prototype.hasOwnProperty.call(b, 'code') ? String(b.code || '').trim() : old.code,
        name: Object.prototype.hasOwnProperty.call(b, 'name') ? String(b.name || '').trim() : old.name,
        legal_basis: Object.prototype.hasOwnProperty.call(b, 'legal_basis') ? (b.legal_basis != null ? String(b.legal_basis) : null) : old.legal_basis,
        trigger_event: Object.prototype.hasOwnProperty.call(b, 'trigger_event') ? String(b.trigger_event || '').trim() : old.trigger_event,
        retention_years: Object.prototype.hasOwnProperty.call(b, 'retention_years')
          ? (b.retention_years != null && b.retention_years !== '' ? parseInt(b.retention_years, 10) : null)
          : old.retention_years,
        is_permanent: Object.prototype.hasOwnProperty.call(b, 'is_permanent') ? (b.is_permanent ? 1 : 0) : old.is_permanent,
        disposal_action: Object.prototype.hasOwnProperty.call(b, 'disposal_action') ? String(b.disposal_action || '').trim() : old.disposal_action,
        review_action_approve: Object.prototype.hasOwnProperty.call(b, 'review_action_approve') ? (b.review_action_approve ? String(b.review_action_approve) : null) : old.review_action_approve,
        review_action_retain: Object.prototype.hasOwnProperty.call(b, 'review_action_retain') ? (b.review_action_retain ? String(b.review_action_retain) : null) : old.review_action_retain,
        confirmation_days: Object.prototype.hasOwnProperty.call(b, 'confirmation_days') ? parseInt(b.confirmation_days, 10) : old.confirmation_days,
        alert_recipients: Object.prototype.hasOwnProperty.call(b, 'alert_recipients') ? (b.alert_recipients != null ? String(b.alert_recipients) : null) : old.alert_recipients,
        notes: Object.prototype.hasOwnProperty.call(b, 'notes') ? (b.notes != null ? String(b.notes) : null) : old.notes,
        is_active: Object.prototype.hasOwnProperty.call(b, 'is_active') ? (b.is_active ? 1 : 0) : old.is_active,
      };
      if (!next.code || !next.name || !next.trigger_event || !next.disposal_action) {
        return res.status(400).json({ error: 'code, name, trigger_event, disposal_action là bắt buộc' });
      }
      if (activeUsers.length > 0) {
        return res.status(409).json({
          error: 'Không thể sửa disposal schedule vì đang có records active sử dụng',
          affected_records: activeUsers,
        });
      }

      const run = db.transaction(() => {
        db.prepare(`
          UPDATE rm_disposal_schedules
          SET code=@code, name=@name, legal_basis=@legal_basis, trigger_event=@trigger_event,
              retention_years=@retention_years, is_permanent=@is_permanent,
              disposal_action=@disposal_action, review_action_approve=@review_action_approve,
              review_action_retain=@review_action_retain, confirmation_days=@confirmation_days,
              alert_recipients=@alert_recipients, notes=@notes, is_active=@is_active,
              updated_at=datetime('now')
          WHERE id=@id
        `).run({ ...next, id });

        if (old.retention_years !== next.retention_years || old.is_permanent !== next.is_permanent) {
          const records = db.prepare(`
            SELECT id, trigger_date
            FROM rm_records
            WHERE disposal_schedule_id = ?
          `).all(id);
          const up = db.prepare('UPDATE rm_records SET retention_due_date = ?, updated_at = datetime(\'now\') WHERE id = ?');
          for (const r of records) {
            const due = next.is_permanent === 1 ? null : addYearsIsoDate(r.trigger_date, next.retention_years);
            up.run(due, r.id);
          }
        }
      });
      run();

      const updated = db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'disposal_schedule',
        entity_id: id,
        entity_title: updated.name,
        actor_id: req.user.id,
        before_state: old,
        after_state: updated,
      });
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi cập nhật disposal schedule' });
    }
  });

  router.patch('/disposal-schedules/:id/deactivate', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const old = db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(id);
      if (!old) return res.status(404).json({ error: 'Disposal schedule không tồn tại' });
      db.prepare('UPDATE rm_disposal_schedules SET is_active = 0, updated_at = datetime(\'now\') WHERE id = ?').run(id);
      const updated = db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(id);
      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'disposal_schedule',
        entity_id: id,
        entity_title: updated.name,
        actor_id: req.user.id,
        before_state: old,
        after_state: updated,
        payload: { action: 'deactivate' },
      });
      return res.json(updated);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi deactivate disposal schedule' });
    }
  });

  return router;
}

module.exports = createRecordsAdminRouter;
