'use strict';

const crypto = require('crypto');
const express = require('express');
const fs = require('fs');
const multer = require('multer');
const path = require('path');

const {
  generateRecordNumber,
  appendEvent,
} = require('../migrations/0050_records_module.js');

const UPLOAD_ROOT = path.join(process.cwd(), 'uploads', 'records');

const ALLOWED_EXT = new Set([
  '.pdf',
  '.doc',
  '.docx',
  '.xls',
  '.xlsx',
  '.png',
  '.jpg',
  '.jpeg',
]);

const ALLOWED_MIME = new Set([
  'application/pdf',
  'application/msword',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  'application/vnd.ms-excel',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  'image/png',
  'image/jpeg',
]);

const MAX_BYTES = 50 * 1024 * 1024;

function extnameLower(name) {
  const e = path.extname(name || '').toLowerCase();
  if (e === '.jpeg') return '.jpg';
  return e;
}

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

function normalizePrimaryFlags(files) {
  const list = Array.isArray(files) ? files.map((f) => ({ ...f })) : [];
  for (const f of list) {
    f.isPrimary =
      f.isPrimary === true ||
      f.isPrimary === 1 ||
      f.isPrimary === '1' ||
      f.isPrimary === 'true';
  }
  let found = false;
  for (const f of list) {
    if (f.isPrimary) {
      if (found) f.isPrimary = false;
      else found = true;
    }
  }
  if (list.length && !found) list[0].isPrimary = true;
  return list;
}

function normalizeComponentLabel(label) {
  const allowed = new Set(['signed', 'draft', 'annex', 'other']);
  if (label && allowed.has(label)) return label;
  return null;
}

/**
 * @param {import('better-sqlite3').Database} db
 * @param {import('express').RequestHandler} requireAuth — middleware JWT, gắn req.user.id
 */
function createRecordsCaptureRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '2mb' }));
  router.use(requireAuth);

  const storage = multer.diskStorage({
    destination(_req, _file, cb) {
      const now = new Date();
      const y = String(now.getFullYear());
      const m = String(now.getMonth() + 1).padStart(2, '0');
      const dir = path.join(UPLOAD_ROOT, y, m);
      fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename(_req, file, cb) {
      const ext = extnameLower(file.originalname);
      cb(null, `${crypto.randomUUID()}${ext}`);
    },
  });

  const upload = multer({
    storage,
    limits: { fileSize: MAX_BYTES },
    fileFilter(_req, file, cb) {
      const ext = extnameLower(file.originalname);
      if (!ALLOWED_EXT.has(ext)) {
        return cb(new Error('Định dạng file không được phép'));
      }
      const mt = (file.mimetype || '').toLowerCase();
      if (
        !ALLOWED_MIME.has(mt) &&
        !(mt === 'application/octet-stream' && ALLOWED_EXT.has(ext))
      ) {
        return cb(new Error('Loại MIME không được phép'));
      }
      cb(null, true);
    },
  });

  function handleMulterError(err, _req, res, next) {
    if (!err) return next();
    if (err instanceof multer.MulterError) {
      if (err.code === 'LIMIT_FILE_SIZE') {
        return res.status(400).json({ error: 'Mỗi file tối đa 50MB' });
      }
      return res.status(400).json({ error: err.message || 'Lỗi upload' });
    }
    if (err.message === 'Định dạng file không được phép' ||
        err.message === 'Loại MIME không được phép') {
      return res.status(400).json({ error: err.message });
    }
    return next(err);
  }

  router.post(
    '/upload',
    upload.array('files'),
    handleMulterError,
    (req, res) => {
      try {
        const files = req.files || [];
        if (!files.length) {
          return res.status(400).json({ error: 'Không có file (field name: files)' });
        }
        const out = files.map((f) => {
          const sha256 = hashFileSync(f.path);
          const rel = path
            .relative(UPLOAD_ROOT, f.path)
            .split(path.sep)
            .join('/');
          return {
            originalName: f.originalname,
            storedName: rel,
            mimeType: f.mimetype,
            size: f.size,
            sha256,
            tempId: crypto.randomUUID(),
          };
        });
        return res.json(out);
      } catch (e) {
        return res.status(500).json({ error: e.message || 'Lỗi xử lý upload' });
      }
    }
  );

  router.get('/classifications', (_req, res) => {
    try {
      const rows = db
        .prepare(
          `
        SELECT
          c.id,
          c.code,
          c.name,
          c.description,
          c.parent_id,
          c.disposal_schedule_id,
          c.inherit_disposal,
          c.is_open,
          c.sort_order,
          c.created_at,
          c.updated_at,
          d.name AS disposal_schedule_name,
          d.code AS disposal_schedule_code,
          d.retention_years AS schedule_retention_years,
          d.is_permanent AS schedule_is_permanent,
          d.trigger_event AS schedule_trigger_event,
          (SELECT COUNT(1) FROM rm_records r WHERE r.classification_id = c.id) AS record_count
        FROM rm_classifications c
        LEFT JOIN rm_disposal_schedules d ON d.id = c.disposal_schedule_id
        ORDER BY c.sort_order, c.code
        `
        )
        .all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi truy vấn phân loại' });
    }
  });

  router.get('/disposal-schedules', (_req, res) => {
    try {
      const rows = db
        .prepare(
          `
        SELECT id, code, name, retention_years, is_permanent, trigger_event, disposal_action
        FROM rm_disposal_schedules
        WHERE is_active = 1
        ORDER BY code
        `
        )
        .all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi truy vấn lịch lưu giữ' });
    }
  });

  router.get('/aggregations', (_req, res) => {
    try {
      const rows = db
        .prepare(
          `
        SELECT id, code, title, agg_type, classification_id, is_open
        FROM rm_aggregations
        WHERE is_open = 1
        ORDER BY code
        `
        )
        .all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi truy vấn aggregation' });
    }
  });

  router.get('/tags', (_req, res) => {
    try {
      const rows = db
        .prepare(
          `SELECT id, name, color_class FROM rm_tags ORDER BY name`
        )
        .all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi truy vấn tag' });
    }
  });

  router.post('/finalize', (req, res) => {
    const userId = req.user && req.user.id;
    if (!userId) {
      return res.status(401).json({ error: 'Thiếu thông tin người dùng (req.user.id)' });
    }

    const body = req.body || {};
    const errors = [];

    const title = (body.title || '').trim();
    const classificationId = (body.classificationId || '').trim();
    const docType = (body.docType || '').trim();
    const issueDate = (body.issueDate || '').trim();
    const author = (body.author || '').trim();
    const issuingUnit = (body.issuingUnit || '').trim();

    if (!title) errors.push('Tiêu đề là bắt buộc');
    if (!classificationId) errors.push('Phân loại là bắt buộc');
    if (!docType) errors.push('Loại văn bản là bắt buộc');
    if (!issueDate) errors.push('Ngày ban hành là bắt buộc');
    if (!author) errors.push('Người soạn là bắt buộc');
    if (!issuingUnit) errors.push('Đơn vị ban hành là bắt buộc');

    const uploadedFiles = normalizePrimaryFlags(body.uploadedFiles || []);
    if (!uploadedFiles.length) errors.push('Cần ít nhất một file đã upload');

    const disposalScheduleId = (body.disposalScheduleId || '').trim();
    if (!disposalScheduleId) errors.push('Disposal schedule là bắt buộc');

    const triggerEvent = (body.triggerEvent || '').trim();
    if (!['issue_date', 'expiry_date', 'custom'].includes(triggerEvent)) {
      errors.push('triggerEvent không hợp lệ');
    }

    const expiryDate = body.expiryDate ? String(body.expiryDate).trim() : null;
    const triggerDateOverride = body.triggerDateOverride
      ? String(body.triggerDateOverride).trim()
      : null;

    if (triggerEvent === 'expiry_date' && !expiryDate) {
      errors.push('Ngày hết hiệu lực bắt buộc khi trigger là expiry_date');
    }
    if (triggerEvent === 'custom' && !triggerDateOverride) {
      errors.push('Ngày trigger tùy chỉnh bắt buộc khi trigger là custom');
    }

    if (errors.length) {
      return res.status(400).json({ error: errors.join('; ') });
    }

    let triggerDate;
    if (triggerEvent === 'issue_date') triggerDate = issueDate;
    else if (triggerEvent === 'expiry_date') triggerDate = expiryDate;
    else triggerDate = triggerDateOverride;

    const cl = db
      .prepare(`SELECT id FROM rm_classifications WHERE id = ?`)
      .get(classificationId);
    if (!cl) {
      return res.status(400).json({ error: 'Phân loại không tồn tại' });
    }

    const ds = db
      .prepare(
        `SELECT id, retention_years, is_permanent FROM rm_disposal_schedules WHERE id = ? AND is_active = 1`
      )
      .get(disposalScheduleId);
    if (!ds) {
      return res.status(400).json({ error: 'Lịch lưu giữ không tồn tại hoặc không active' });
    }

    const aggregationId =
      body.aggregationId && String(body.aggregationId).trim()
        ? String(body.aggregationId).trim()
        : null;
    if (aggregationId) {
      const agg = db.prepare(`SELECT id FROM rm_aggregations WHERE id = ?`).get(aggregationId);
      if (!agg) {
        return res.status(400).json({ error: 'Aggregation không tồn tại' });
      }
    }

    const tagIds = Array.isArray(body.tagIds) ? body.tagIds.map(String) : [];
    for (const tid of tagIds) {
      const t = db.prepare(`SELECT id FROM rm_tags WHERE id = ?`).get(tid);
      if (!t) {
        return res.status(400).json({ error: `Tag không tồn tại: ${tid}` });
      }
    }

    for (const f of uploadedFiles) {
      if (!f.storedName || !f.originalName || !f.mimeType || f.size == null || !f.sha256) {
        return res.status(400).json({ error: 'Thiếu trường trong uploadedFiles' });
      }
      if (!Number.isFinite(Number(f.size)) || Number(f.size) <= 0) {
        return res.status(400).json({ error: `Dung lượng component không hợp lệ: ${f.originalName}` });
      }
      if (!/^[a-f0-9]{64}$/i.test(String(f.sha256))) {
        return res.status(400).json({ error: `SHA256 component không hợp lệ: ${f.originalName}` });
      }
      let abs;
      try {
        abs = resolveStoredPath(f.storedName);
      } catch (e) {
        return res.status(400).json({ error: e.message });
      }
      if (!fs.existsSync(abs)) {
        return res.status(400).json({ error: `Không tìm thấy file: ${f.storedName}` });
      }
      const diskHash = hashFileSync(abs);
      if (diskHash !== f.sha256) {
        return res.status(400).json({
          error: `SHA256 không khớp cho file ${f.originalName}`,
        });
      }
    }

    const retentionDueDate =
      ds.is_permanent === 1
        ? null
        : addYearsIsoDate(triggerDate, ds.retention_years);

    const recordId = crypto.randomUUID();
    const docRef = body.docRef ? String(body.docRef).trim() : null;
    const effectiveDate = body.effectiveDate ? String(body.effectiveDate).trim() : null;
    const approver = body.approver ? String(body.approver).trim() : null;
    const description = body.description ? String(body.description).trim() : null;
    const keywords = body.keywords ? String(body.keywords).trim() : null;
    const isConfidential = body.isConfidential === true || body.isConfidential === 1 || body.isConfidential === '1'
      ? 1
      : 0;

    const insRecord = db.prepare(`
      INSERT INTO rm_records (
        id, record_number, doc_ref, title, doc_type,
        classification_id, aggregation_id, disposal_schedule_id,
        issue_date, effective_date, expiry_date, trigger_date, retention_due_date,
        author, approver, issuing_unit, captured_by, capture_date,
        description, keywords, state, is_frozen, is_vital, is_confidential
      ) VALUES (
        @id, @record_number, @doc_ref, @title, @doc_type,
        @classification_id, @aggregation_id, @disposal_schedule_id,
        @issue_date, @effective_date, @expiry_date, @trigger_date, @retention_due_date,
        @author, @approver, @issuing_unit, @captured_by, datetime('now'),
        @description, @keywords, 'active', 1, 0, @is_confidential
      )
    `);

    const insComp = db.prepare(`
      INSERT INTO rm_components (
        id, record_id, filename, stored_filename, mime_type, file_size, sha256_hash,
        version, is_primary, component_label, uploaded_by
      ) VALUES (
        @id, @record_id, @filename, @stored_filename, @mime_type, @file_size, @sha256_hash,
        '1.0', @is_primary, @component_label, @uploaded_by
      )
    `);

    const insTag = db.prepare(`
      INSERT INTO rm_record_tags (record_id, tag_id) VALUES (@record_id, @tag_id)
    `);

    try {
      let record_number;
      const run = db.transaction(() => {
        record_number = generateRecordNumber(db);
        insRecord.run({
          id: recordId,
          record_number,
          doc_ref: docRef,
          title,
          doc_type: docType,
          classification_id: classificationId,
          aggregation_id: aggregationId,
          disposal_schedule_id: disposalScheduleId,
          issue_date: issueDate,
          effective_date: effectiveDate,
          expiry_date: expiryDate,
          trigger_date: triggerDate,
          retention_due_date: retentionDueDate,
          author,
          approver,
          issuing_unit: issuingUnit,
          captured_by: userId,
          description,
          keywords,
          is_confidential: isConfidential,
        });

        for (const f of uploadedFiles) {
          insComp.run({
            id: crypto.randomUUID(),
            record_id: recordId,
            filename: f.originalName,
            stored_filename: f.storedName,
            mime_type: f.mimeType,
            file_size: Number(f.size),
            sha256_hash: f.sha256,
            is_primary: f.isPrimary ? 1 : 0,
            component_label: normalizeComponentLabel(f.label),
            uploaded_by: userId,
          });
        }

        for (const tid of tagIds) {
          insTag.run({ record_id: recordId, tag_id: tid });
        }

        appendEvent(db, {
          event_type: 'capture',
          entity_type: 'record',
          entity_id: recordId,
          entity_title: title,
          actor_id: userId,
          payload: JSON.stringify({
            record_number,
            classificationId,
            disposalScheduleId,
            fileCount: uploadedFiles.length,
          }),
        });
      });

      run();

      const record = db.prepare(`SELECT * FROM rm_records WHERE id = ?`).get(recordId);
      return res.status(201).json({ record });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi khi phong bế record' });
    }
  });

  return router;
}

module.exports = createRecordsCaptureRouter;
