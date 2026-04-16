'use strict';

const express = require('express');
const { appendEvent } = require('../migrations/0050_records_module.js');

const SORTABLE_FIELDS = {
  capture_date: 'r.capture_date',
  issue_date: 'r.issue_date',
  retention_due_date: 'r.retention_due_date',
  title: 'r.title',
  state: 'r.state',
};

function toInt(value, fallback) {
  const n = parseInt(value, 10);
  if (Number.isNaN(n)) return fallback;
  return n;
}

function parseTagArray(value) {
  if (!value) return [];
  try {
    const arr = JSON.parse(value);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function pickEditableFields(row) {
  return {
    description: row.description,
    keywords: row.keywords,
    approver: row.approver,
    notes: row.disposal_notes,
  };
}

function createRecordsRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '2mb' }));
  router.use(requireAuth);

  router.get('/', (req, res) => {
    try {
      const page = Math.max(1, toInt(req.query.page, 1));
      const limit = Math.min(100, Math.max(1, toInt(req.query.limit, 20)));
      const offset = (page - 1) * limit;

      const sortBy = req.query.sortBy && SORTABLE_FIELDS[req.query.sortBy]
        ? req.query.sortBy
        : 'capture_date';
      const sortDir = String(req.query.sortDir || 'desc').toLowerCase() === 'asc' ? 'ASC' : 'DESC';
      const orderByExpr = SORTABLE_FIELDS[sortBy];

      const where = [];
      const params = {};

      if (req.query.search) {
        where.push('(r.title LIKE @search OR r.doc_ref LIKE @search OR r.record_number LIKE @search)');
        params.search = `%${String(req.query.search).trim()}%`;
      }
      if (req.query.state) {
        where.push('r.state = @state');
        params.state = String(req.query.state).trim();
      }
      if (req.query.docType) {
        where.push('r.doc_type = @docType');
        params.docType = String(req.query.docType).trim();
      }
      if (req.query.classificationId) {
        where.push('r.classification_id = @classificationId');
        params.classificationId = String(req.query.classificationId).trim();
      }
      if (req.query.year) {
        where.push('substr(r.issue_date, 1, 4) = @year');
        params.year = String(req.query.year).trim();
      }
      if (req.query.tagId) {
        where.push('EXISTS (SELECT 1 FROM rm_record_tags f WHERE f.record_id = r.id AND f.tag_id = @tagId)');
        params.tagId = String(req.query.tagId).trim();
      }

      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

      const total = db
        .prepare(`SELECT COUNT(1) AS c FROM rm_records r ${whereSql}`)
        .get(params).c;

      const rows = db
        .prepare(
          `
          SELECT
            r.*,
            c.name AS classification_name,
            ds.code AS disposal_schedule_code,
            a.title AS aggregation_title,
            CAST(julianday(date(r.retention_due_date)) - julianday(date('now')) AS INTEGER) AS days_to_disposal,
            (
              SELECT json_group_array(json_object('id', t.id, 'name', t.name, 'color_class', t.color_class))
              FROM rm_record_tags rt
              JOIN rm_tags t ON t.id = rt.tag_id
              WHERE rt.record_id = r.id
            ) AS tags_json
          FROM rm_records r
          LEFT JOIN rm_classifications c ON c.id = r.classification_id
          LEFT JOIN rm_disposal_schedules ds ON ds.id = r.disposal_schedule_id
          LEFT JOIN rm_aggregations a ON a.id = r.aggregation_id
          ${whereSql}
          ORDER BY ${orderByExpr} ${sortDir}, r.id DESC
          LIMIT @limit OFFSET @offset
          `
        )
        .all({ ...params, limit, offset });

      const records = rows.map((r) => ({
        ...r,
        tags: parseTagArray(r.tags_json),
      }));
      for (const r of records) delete r.tags_json;

      return res.json({
        records,
        total,
        page,
        totalPages: Math.max(1, Math.ceil(total / limit)),
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không tải được danh sách record' });
    }
  });

  router.get('/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const record = db.prepare(`SELECT * FROM rm_records WHERE id = ?`).get(id);
      if (!record) {
        return res.status(404).json({ error: 'Record không tồn tại' });
      }

      const components = db
        .prepare(
          `
          SELECT id, filename, stored_filename, mime_type, file_size, sha256_hash, version, is_primary, component_label, uploaded_at
          FROM rm_components
          WHERE record_id = ?
          ORDER BY is_primary DESC, uploaded_at ASC
          `
        )
        .all(id);

      const tags = db
        .prepare(
          `
          SELECT t.id, t.name, t.color_class
          FROM rm_record_tags rt
          JOIN rm_tags t ON t.id = rt.tag_id
          WHERE rt.record_id = ?
          ORDER BY t.name
          `
        )
        .all(id);

      const classificationPath = record.classification_id
        ? db
            .prepare(
              `
              WITH RECURSIVE cte AS (
                SELECT id, code, name, parent_id, 0 AS depth
                FROM rm_classifications
                WHERE id = @leaf
                UNION ALL
                SELECT p.id, p.code, p.name, p.parent_id, cte.depth + 1
                FROM rm_classifications p
                JOIN cte ON cte.parent_id = p.id
              )
              SELECT id, code, name, parent_id
              FROM cte
              ORDER BY depth DESC
              `
            )
            .all({ leaf: record.classification_id })
        : [];

      const acl = db
        .prepare(
          `
          SELECT a.id, a.entity_type, a.entity_id, a.user_id, a.permission, a.granted_by, a.granted_at, u.name AS user_name
          FROM rm_acl a
          LEFT JOIN users u ON u.id = a.user_id
          WHERE a.entity_type = 'record' AND a.entity_id = ?
          ORDER BY a.granted_at DESC
          `
        )
        .all(id);

      const disposalWorkflow = db
        .prepare(`SELECT * FROM rm_disposal_workflow WHERE record_id = ?`)
        .get(id) || null;

      const eventHistory = db
        .prepare(
          `
          SELECT *
          FROM rm_event_history
          WHERE entity_type = 'record' AND entity_id = ?
          ORDER BY occurred_at DESC, id DESC
          LIMIT 10
          `
        )
        .all(id);

      const payload = {
        ...record,
        components,
        tags,
        classification_path: classificationPath,
        acl,
        disposal_workflow: disposalWorkflow,
        event_history: eventHistory,
      };

      res.json(payload);

      // Fire-and-forget, không block response
      setImmediate(() => {
        try {
          appendEvent(db, {
            event_type: 'view',
            entity_type: 'record',
            entity_id: id,
            entity_title: record.title,
            actor_id: req.user && req.user.id ? req.user.id : null,
            payload: JSON.stringify({ source: 'records_detail_api' }),
          });
        } catch (_e) {
          // ignore logging failures
        }
      });
      return undefined;
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không tải được record' });
    }
  });

  router.patch('/:id', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const record = db.prepare(`SELECT * FROM rm_records WHERE id = ?`).get(id);
      if (!record) {
        return res.status(404).json({ error: 'Record không tồn tại' });
      }

      const body = req.body || {};
      const forbidden = ['title', 'docRef', 'doc_ref', 'classificationId', 'classification_id', 'issue_date', 'is_frozen'];
      for (const key of forbidden) {
        if (Object.prototype.hasOwnProperty.call(body, key) && record.is_frozen === 1) {
          return res.status(400).json({ error: `Record đã frozen, không được sửa trường ${key}` });
        }
      }

      const allowedKeys = ['description', 'keywords', 'approver', 'notes', 'tagIds'];
      for (const key of Object.keys(body)) {
        if (!allowedKeys.includes(key)) {
          return res.status(400).json({ error: `Trường không được phép cập nhật: ${key}` });
        }
      }

      const next = {
        description: Object.prototype.hasOwnProperty.call(body, 'description') ? body.description : record.description,
        keywords: Object.prototype.hasOwnProperty.call(body, 'keywords') ? body.keywords : record.keywords,
        approver: Object.prototype.hasOwnProperty.call(body, 'approver') ? body.approver : record.approver,
        notes: Object.prototype.hasOwnProperty.call(body, 'notes') ? body.notes : record.disposal_notes,
      };
      const nextTagIds = Array.isArray(body.tagIds) ? body.tagIds.map(String) : null;

      if (nextTagIds) {
        for (const tagId of nextTagIds) {
          const tag = db.prepare(`SELECT id FROM rm_tags WHERE id = ?`).get(tagId);
          if (!tag) return res.status(400).json({ error: `Tag không tồn tại: ${tagId}` });
        }
      }

      const before = pickEditableFields(record);
      const beforeTagIds = db.prepare(`SELECT tag_id FROM rm_record_tags WHERE record_id = ? ORDER BY tag_id`).all(id).map((x) => x.tag_id);

      const tx = db.transaction(() => {
        db.prepare(
          `
          UPDATE rm_records
          SET
            description = @description,
            keywords = @keywords,
            approver = @approver,
            disposal_notes = @notes,
            updated_at = datetime('now')
          WHERE id = @id
          `
        ).run({
          id,
          description: next.description != null ? String(next.description) : null,
          keywords: next.keywords != null ? String(next.keywords) : null,
          approver: next.approver != null ? String(next.approver) : null,
          notes: next.notes != null ? String(next.notes) : null,
        });

        if (nextTagIds) {
          db.prepare(`DELETE FROM rm_record_tags WHERE record_id = ?`).run(id);
          const ins = db.prepare(`INSERT INTO rm_record_tags (record_id, tag_id) VALUES (?, ?)`);
          for (const tagId of nextTagIds) ins.run(id, tagId);
        }
      });
      tx();

      const updated = db.prepare(`SELECT * FROM rm_records WHERE id = ?`).get(id);
      const tags = db
        .prepare(
          `
          SELECT t.id, t.name, t.color_class
          FROM rm_record_tags rt
          JOIN rm_tags t ON t.id = rt.tag_id
          WHERE rt.record_id = ?
          ORDER BY t.name
          `
        )
        .all(id);

      const after = pickEditableFields(updated);
      const afterTagIds = tags.map((t) => t.id).sort();

      appendEvent(db, {
        event_type: 'metadata_update',
        entity_type: 'record',
        entity_id: id,
        entity_title: updated.title,
        actor_id: req.user && req.user.id ? req.user.id : null,
        before_state: JSON.stringify({ ...before, tagIds: beforeTagIds }),
        after_state: JSON.stringify({ ...after, tagIds: afterTagIds }),
        payload: JSON.stringify({ updated_fields: Object.keys(body) }),
      });

      return res.json({ ...updated, tags });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không cập nhật được metadata' });
    }
  });

  return router;
}

module.exports = createRecordsRouter;
