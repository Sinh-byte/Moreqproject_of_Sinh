'use strict';

const crypto = require('crypto');
const express = require('express');
const fs = require('fs');
const multer = require('multer');
const path = require('path');
const { appendEvent } = require('../migrations/0050_records_module.js');

const SORTABLE_FIELDS = {
  capture_date: 'r.capture_date',
  issue_date: 'r.issue_date',
  retention_due_date: 'r.retention_due_date',
  title: 'r.title',
  state: 'r.state',
};
const UPLOAD_ROOT = path.join(process.cwd(), 'uploads', 'records');

function hashFileSync(absPath) {
  const h = crypto.createHash('sha256');
  h.update(fs.readFileSync(absPath));
  return h.digest('hex');
}

function resolveStoredPath(storedName) {
  if (!storedName || typeof storedName !== 'string' || storedName.includes('..')) {
    throw new Error('Đường dẫn file không hợp lệ');
  }
  const normalized = path.normalize(storedName).replace(/^(\.\.(\/|\\|$))+/, '');
  const base = path.resolve(UPLOAD_ROOT);
  const full = path.resolve(base, normalized);
  const rel = path.relative(base, full);
  if (rel.startsWith('..') || path.isAbsolute(rel)) {
    throw new Error('Đường dẫn file không nằm trong thư mục upload');
  }
  return full;
}

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

function normalizeIdArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.map((x) => String(x || '').trim()).filter(Boolean);
  return String(value)
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
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
  router.use(requireAuth.requirePermission('records.view'));
  const componentUpload = multer({
    storage: multer.diskStorage({
      destination(_req, _file, cb) {
        const now = new Date();
        const y = String(now.getFullYear());
        const m = String(now.getMonth() + 1).padStart(2, '0');
        const dir = path.join(UPLOAD_ROOT, y, m);
        fs.mkdirSync(dir, { recursive: true });
        cb(null, dir);
      },
      filename(_req, file, cb) {
        const ext = path.extname(file.originalname || '').toLowerCase();
        cb(null, `${crypto.randomUUID()}${ext}`);
      },
    }),
    limits: { fileSize: 50 * 1024 * 1024 },
  });

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
      if (req.query.title) {
        where.push('r.title LIKE @title');
        params.title = `%${String(req.query.title).trim()}%`;
      }
      if (req.query.docRef) {
        where.push('(r.doc_ref LIKE @docRef OR r.record_number LIKE @docRef)');
        params.docRef = `%${String(req.query.docRef).trim()}%`;
      }
      if (req.query.state) {
        where.push('r.state = @state');
        params.state = String(req.query.state).trim();
      }
      if (req.query.docType) {
        where.push('r.doc_type = @docType');
        params.docType = String(req.query.docType).trim();
      }
      if (req.query.issueDateFrom) {
        where.push('date(r.issue_date) >= date(@issueDateFrom)');
        params.issueDateFrom = String(req.query.issueDateFrom).trim();
      }
      if (req.query.issueDateTo) {
        where.push('date(r.issue_date) <= date(@issueDateTo)');
        params.issueDateTo = String(req.query.issueDateTo).trim();
      }
      if (req.query.classificationId) {
        const classificationId = String(req.query.classificationId).trim();
        const useDescendants = String(req.query.classificationScope || '').trim() === 'descendants';
        if (useDescendants) {
          where.push(`
            r.classification_id IN (
              WITH RECURSIVE class_tree AS (
                SELECT id, parent_id FROM rm_classifications WHERE id = @classificationId
                UNION ALL
                SELECT c.id, c.parent_id
                FROM rm_classifications c
                JOIN class_tree t ON c.parent_id = t.id
              )
              SELECT id FROM class_tree
            )
          `);
        } else {
          where.push('r.classification_id = @classificationId');
        }
        params.classificationId = classificationId;
      }
      if (req.query.year) {
        where.push('substr(r.issue_date, 1, 4) = @year');
        params.year = String(req.query.year).trim();
      }
      if (req.query.tagId) {
        where.push('EXISTS (SELECT 1 FROM rm_record_tags f WHERE f.record_id = r.id AND f.tag_id = @tagId)');
        params.tagId = String(req.query.tagId).trim();
      }
      const tagIds = normalizeIdArray(req.query.tagIds);
      if (tagIds.length) {
        const tagCond = tagIds.map((_, idx) => `EXISTS (
          SELECT 1 FROM rm_record_tags tf${idx}
          WHERE tf${idx}.record_id = r.id
            AND tf${idx}.tag_id = @tagIdList_${idx}
        )`).join(' AND ');
        tagIds.forEach((tagId, idx) => {
          params[`tagIdList_${idx}`] = tagId;
        });
        where.push(`(${tagCond})`);
      }

      const principals = requireAuth.userPrincipalsFromReq(req);
      const aclBypass = requireAuth.hasPermission(req, 'records.view_all');
      if (!aclBypass) {
        if (!principals.length) {
          return res.json({ records: [], total: 0, page, totalPages: 1 });
        }
        const recordAclCond = principals.map((_, idx) => (
          `(a.principal_type = @acl_type_${idx} AND a.principal_id = @acl_id_${idx})`
        )).join(' OR ');
        const aggregationAclCond = principals.map((_, idx) => (
          `(aa.principal_type = @acl_type_${idx} AND aa.principal_id = @acl_id_${idx})`
        )).join(' OR ');
        for (let idx = 0; idx < principals.length; idx += 1) {
          params[`acl_type_${idx}`] = principals[idx].principal_type;
          params[`acl_id_${idx}`] = principals[idx].principal_id;
        }
        where.push(`(
          EXISTS (
            SELECT 1
            FROM rm_record_acl a
            WHERE a.entity_type = 'record'
              AND a.entity_id = r.id
              AND (a.permission = 'view' OR a.permission = 'acl_manage')
              AND (${recordAclCond})
          )
          OR EXISTS (
            SELECT 1
            FROM rm_record_acl aa
            WHERE aa.entity_type = 'aggregation'
              AND aa.entity_id = r.aggregation_id
              AND (aa.permission = 'view' OR aa.permission = 'acl_manage')
              AND (${aggregationAclCond})
          )
        )`);
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

      if (
        req.query.search || req.query.title || req.query.docRef || req.query.issueDateFrom
        || req.query.issueDateTo || req.query.tagIds || req.query.classificationScope === 'descendants'
      ) {
        setImmediate(() => {
          try {
            appendEvent(db, {
              event_type: 'search',
              entity_type: 'record',
              entity_id: 'query',
              actor_id: req.user && req.user.id ? req.user.id : null,
              payload: {
                keyword: String(req.query.search).trim(),
                title: req.query.title ? String(req.query.title).trim() : null,
                docRef: req.query.docRef ? String(req.query.docRef).trim() : null,
                issueDateFrom: req.query.issueDateFrom ? String(req.query.issueDateFrom).trim() : null,
                issueDateTo: req.query.issueDateTo ? String(req.query.issueDateTo).trim() : null,
                tagIds,
                classificationId: req.query.classificationId ? String(req.query.classificationId).trim() : null,
                classificationScope: req.query.classificationScope ? String(req.query.classificationScope).trim() : 'exact',
                page,
                limit,
                result_count: records.length,
                total,
              },
            });
          } catch (_e) {
            // ignore audit failures for search telemetry
          }
        });
      }

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
      if (!requireAuth.hasAclPermission(req, 'record', id, 'view')) {
        return res.status(403).json({ error: 'Bạn không có quyền xem record này' });
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
      const canViewConfidential = !record.is_confidential || requireAuth.hasPermission(req, 'view_confidential');
      const safeComponents = canViewConfidential
        ? components
        : components.map((c) => ({
            ...c,
            stored_filename: null,
            is_redacted: 1,
          }));

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
          SELECT
            a.id, a.entity_type, a.entity_id, a.principal_type, a.principal_id,
            a.permission, a.granted_by, a.granted_at,
            u.name AS user_name
          FROM rm_record_acl a
          LEFT JOIN users u ON (a.principal_type = 'user' AND u.id = a.principal_id)
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
        components: safeComponents,
        confidentiality: {
          is_confidential: record.is_confidential === 1,
          can_view_content: !!canViewConfidential,
          redacted: record.is_confidential === 1 && !canViewConfidential,
        },
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
      if (Object.prototype.hasOwnProperty.call(body, 'isConfidential')) {
        if (!requireAuth.hasPermission(req, 'view_confidential')) {
          return res.status(403).json({ error: 'Thiếu quyền cập nhật confidentiality' });
        }
        allowedKeys.push('isConfidential');
      }
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
            is_confidential = @is_confidential,
            updated_at = datetime('now')
          WHERE id = @id
          `
        ).run({
          id,
          description: next.description != null ? String(next.description) : null,
          keywords: next.keywords != null ? String(next.keywords) : null,
          approver: next.approver != null ? String(next.approver) : null,
          notes: next.notes != null ? String(next.notes) : null,
          is_confidential: Object.prototype.hasOwnProperty.call(body, 'isConfidential')
            ? (body.isConfidential ? 1 : 0)
            : (record.is_confidential === 1 ? 1 : 0),
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

  router.post('/:id/components', componentUpload.single('file'), (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const record = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
      if (!record) return res.status(404).json({ error: 'Record không tồn tại' });
      if (record.is_frozen === 1) return res.status(400).json({ error: 'Record đã frozen, không thể thêm component' });
      if (!req.file) return res.status(400).json({ error: 'Thiếu file component (field: file)' });

      const rel = path.relative(UPLOAD_ROOT, req.file.path).split(path.sep).join('/');
      const sha256 = hashFileSync(req.file.path);
      const currentCount = db.prepare('SELECT COUNT(1) AS c FROM rm_components WHERE record_id = ?').get(id).c;
      const forcePrimary = String((req.body && req.body.isPrimary) || '').toLowerCase();
      const isPrimary = currentCount === 0 || forcePrimary === '1' || forcePrimary === 'true';
      if (isPrimary) {
        db.prepare('UPDATE rm_components SET is_primary = 0 WHERE record_id = ?').run(id);
      }
      const componentId = crypto.randomUUID();
      db.prepare(`
        INSERT INTO rm_components (
          id, record_id, filename, stored_filename, mime_type, file_size, sha256_hash,
          version, is_primary, component_label, uploaded_by, uploaded_at
        ) VALUES (
          @id, @record_id, @filename, @stored_filename, @mime_type, @file_size, @sha256_hash,
          @version, @is_primary, @component_label, @uploaded_by, datetime('now')
        )
      `).run({
        id: componentId,
        record_id: id,
        filename: req.file.originalname,
        stored_filename: rel,
        mime_type: req.file.mimetype || 'application/octet-stream',
        file_size: req.file.size,
        sha256_hash: sha256,
        version: String((req.body && req.body.version) || '1.0').trim() || '1.0',
        is_primary: isPrimary ? 1 : 0,
        component_label: req.body && req.body.label ? String(req.body.label).trim() : null,
        uploaded_by: req.user && req.user.id ? req.user.id : null,
      });
      const created = db.prepare('SELECT * FROM rm_components WHERE id = ?').get(componentId);
      appendEvent(db, {
        event_type: 'component_add',
        entity_type: 'record',
        entity_id: id,
        entity_title: record.title,
        actor_id: req.user && req.user.id ? req.user.id : null,
        payload: {
          component_id: componentId,
          filename: created.filename,
          mime_type: created.mime_type,
          file_size: created.file_size,
          sha256_hash: created.sha256_hash,
        },
      });
      return res.status(201).json(created);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không thể thêm component' });
    }
  });

  router.delete('/:id/components/:componentId', (req, res) => {
    try {
      const id = String(req.params.id || '').trim();
      const componentId = String(req.params.componentId || '').trim();
      const record = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
      if (!record) return res.status(404).json({ error: 'Record không tồn tại' });
      if (record.is_frozen === 1) return res.status(400).json({ error: 'Record đã frozen, không thể xóa component' });
      const component = db.prepare('SELECT * FROM rm_components WHERE id = ? AND record_id = ?').get(componentId, id);
      if (!component) return res.status(404).json({ error: 'Component không tồn tại' });
      const count = db.prepare('SELECT COUNT(1) AS c FROM rm_components WHERE record_id = ?').get(id).c;
      if (count <= 1) {
        return res.status(400).json({ error: 'Record phải còn ít nhất một component' });
      }
      try {
        const abs = resolveStoredPath(component.stored_filename);
        if (fs.existsSync(abs)) fs.unlinkSync(abs);
      } catch (_e) {
        // ignore file cleanup failures
      }
      db.prepare('DELETE FROM rm_components WHERE id = ?').run(componentId);
      if (component.is_primary === 1) {
        db.prepare(`
          UPDATE rm_components
          SET is_primary = 1
          WHERE id = (
            SELECT id FROM rm_components WHERE record_id = ? ORDER BY uploaded_at ASC LIMIT 1
          )
        `).run(id);
      }
      appendEvent(db, {
        event_type: 'component_remove',
        entity_type: 'record',
        entity_id: id,
        entity_title: record.title,
        actor_id: req.user && req.user.id ? req.user.id : null,
        payload: {
          component_id: component.id,
          filename: component.filename,
          sha256_hash: component.sha256_hash,
        },
      });
      return res.json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không thể xóa component' });
    }
  });

  router.post('/bulk/classification', (req, res) => {
    try {
      const body = req.body || {};
      const recordIds = normalizeIdArray(body.recordIds);
      const classificationId = String(body.classificationId || '').trim();
      if (!recordIds.length || !classificationId) {
        return res.status(400).json({ error: 'recordIds và classificationId là bắt buộc' });
      }
      const cls = db.prepare('SELECT id, name FROM rm_classifications WHERE id = ?').get(classificationId);
      if (!cls) return res.status(404).json({ error: 'Classification không tồn tại' });
      const updateStmt = db.prepare(`
        UPDATE rm_records
        SET classification_id = @classification_id, updated_at = datetime('now')
        WHERE id = @id
      `);
      const tx = db.transaction(() => {
        const result = { updated: [], skipped: [] };
        for (const id of recordIds) {
          const record = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
          if (!record) {
            result.skipped.push({ id, reason: 'not_found' });
            continue;
          }
          if (!requireAuth.hasAclPermission(req, 'record', id, 'view')) {
            result.skipped.push({ id, reason: 'forbidden' });
            continue;
          }
          if (record.is_frozen === 1) {
            result.skipped.push({ id, reason: 'frozen' });
            continue;
          }
          updateStmt.run({ id, classification_id: classificationId });
          const updated = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
          appendEvent(db, {
            event_type: 'metadata_update',
            entity_type: 'record',
            entity_id: id,
            entity_title: updated.title,
            actor_id: req.user && req.user.id ? req.user.id : null,
            before_state: { classification_id: record.classification_id },
            after_state: { classification_id: updated.classification_id },
            payload: { action: 'bulk_change_classification', classificationId },
          });
          result.updated.push(id);
        }
        return result;
      });
      return res.json(tx());
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không thể bulk change classification' });
    }
  });

  router.post('/bulk/tags', (req, res) => {
    try {
      const body = req.body || {};
      const recordIds = normalizeIdArray(body.recordIds);
      const tagIds = normalizeIdArray(body.tagIds);
      if (!recordIds.length || !tagIds.length) {
        return res.status(400).json({ error: 'recordIds và tagIds là bắt buộc' });
      }
      for (const tagId of tagIds) {
        const tag = db.prepare('SELECT id FROM rm_tags WHERE id = ?').get(tagId);
        if (!tag) return res.status(400).json({ error: `Tag không tồn tại: ${tagId}` });
      }
      const ins = db.prepare('INSERT OR IGNORE INTO rm_record_tags (record_id, tag_id) VALUES (?, ?)');
      const tx = db.transaction(() => {
        const result = { updated: [], skipped: [] };
        for (const id of recordIds) {
          const record = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
          if (!record) {
            result.skipped.push({ id, reason: 'not_found' });
            continue;
          }
          if (!requireAuth.hasAclPermission(req, 'record', id, 'view')) {
            result.skipped.push({ id, reason: 'forbidden' });
            continue;
          }
          for (const tagId of tagIds) ins.run(id, tagId);
          appendEvent(db, {
            event_type: 'metadata_update',
            entity_type: 'record',
            entity_id: id,
            entity_title: record.title,
            actor_id: req.user && req.user.id ? req.user.id : null,
            payload: { action: 'bulk_add_tags', tagIds },
          });
          result.updated.push(id);
        }
        return result;
      });
      return res.json(tx());
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không thể bulk add tags' });
    }
  });

  router.post('/bulk/disposal-review', (req, res) => {
    try {
      const body = req.body || {};
      const recordIds = normalizeIdArray(body.recordIds);
      if (!recordIds.length) {
        return res.status(400).json({ error: 'recordIds là bắt buộc' });
      }
      const tx = db.transaction(() => {
        const result = { queued: [], skipped: [] };
        for (const id of recordIds) {
          const record = db.prepare('SELECT * FROM rm_records WHERE id = ?').get(id);
          if (!record) {
            result.skipped.push({ id, reason: 'not_found' });
            continue;
          }
          if (!requireAuth.hasAclPermission(req, 'record', id, 'view')) {
            result.skipped.push({ id, reason: 'forbidden' });
            continue;
          }
          const existed = db.prepare('SELECT id FROM rm_disposal_workflow WHERE record_id = ?').get(id);
          if (existed) {
            result.skipped.push({ id, reason: 'workflow_exists' });
            continue;
          }
          const days = record.disposal_schedule_id
            ? (db.prepare('SELECT IFNULL(confirmation_days,30) AS d FROM rm_disposal_schedules WHERE id = ?').get(record.disposal_schedule_id) || { d: 30 }).d
            : 30;
          db.prepare(`
            INSERT INTO rm_disposal_workflow (id, record_id, triggered_at, deadline, status)
            VALUES (?, ?, datetime('now'), datetime('now', ?), ?)
          `).run(
            crypto.randomUUID(),
            id,
            `+${Number(days) || 30} days`,
            record.is_frozen === 1 ? 'on_hold' : 'pending_review'
          );
          db.prepare(`
            UPDATE rm_records
            SET state = CASE WHEN is_frozen = 1 THEN state ELSE 'review' END,
                disposal_alert_sent_at = datetime('now'),
                updated_at = datetime('now')
            WHERE id = ?
          `).run(id);
          appendEvent(db, {
            event_type: 'disposal_alert',
            entity_type: 'record',
            entity_id: id,
            entity_title: record.title,
            actor_id: req.user && req.user.id ? req.user.id : null,
            payload: { action: 'bulk_enqueue_disposal_review' },
          });
          result.queued.push(id);
        }
        return result;
      });
      return res.json(tx());
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Không thể bulk enqueue disposal review' });
    }
  });

  return router;
}

module.exports = createRecordsRouter;
