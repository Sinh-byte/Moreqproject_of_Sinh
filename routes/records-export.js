'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const crypto = require('crypto');
const zlib = require('zlib');
const JSZip = require('jszip');
const { appendEvent } = require('../migrations/0050_records_module.js');

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 100 * 1024 * 1024 } });
const importPreviewStore = new Map();
const UPLOAD_ROOT = path.join(process.cwd(), 'uploads', 'records');

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

function esc(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function buildRecordsXml(rows, options, db) {
  const recordXml = rows.map((r) => {
    const components = options.includeComponents
      ? db
          .prepare('SELECT * FROM rm_components WHERE record_id = ? ORDER BY uploaded_at')
          .all(r.id)
      : [];
    const acl = options.includeAcl
      ? db
          .prepare(`
            SELECT principal_type, principal_id, permission
            FROM rm_record_acl
            WHERE entity_type = 'record' AND entity_id = ?
          `)
          .all(r.id)
      : [];
    const events = options.includeEventHistory
      ? db
          .prepare('SELECT event_type, actor_id, occurred_at FROM rm_event_history WHERE entity_type = \'record\' AND entity_id = ? ORDER BY occurred_at DESC LIMIT 200')
          .all(r.id)
      : [];
    const ds = options.includeDisposalSchedule && r.disposal_schedule_id
      ? db.prepare('SELECT * FROM rm_disposal_schedules WHERE id = ?').get(r.disposal_schedule_id)
      : null;
    return `
    <mcrs:record id="${esc(r.id)}" number="${esc(r.record_number)}">
      <mcrs:title>${esc(r.title)}</mcrs:title>
      <mcrs:docType>${esc(r.doc_type)}</mcrs:docType>
      <mcrs:classificationId>${esc(r.classification_id)}</mcrs:classificationId>
      <mcrs:aggregationId>${esc(r.aggregation_id)}</mcrs:aggregationId>
      <mcrs:triggerDate>${esc(r.trigger_date)}</mcrs:triggerDate>
      <mcrs:retentionDueDate>${esc(r.retention_due_date)}</mcrs:retentionDueDate>
      <mcrs:state>${esc(r.state)}</mcrs:state>
      ${ds ? `<mcrs:disposalSchedule id="${esc(ds.id)}" code="${esc(ds.code)}" retentionYears="${esc(ds.retention_years)}" isPermanent="${esc(ds.is_permanent)}" />` : ''}
      ${components.length ? `<mcrs:components>${components.map((c) => `<mcrs:component id="${esc(c.id)}" filename="${esc(c.filename)}" stored="${esc(c.stored_filename)}" mime="${esc(c.mime_type)}" size="${esc(c.file_size)}" sha256="${esc(c.sha256_hash)}" />`).join('')}</mcrs:components>` : ''}
      ${acl.length ? `<mcrs:acl>${acl.map((a) => `<mcrs:grant principalType="${esc(a.principal_type)}" principalId="${esc(a.principal_id)}" permission="${esc(a.permission)}" />`).join('')}</mcrs:acl>` : ''}
      ${events.length ? `<mcrs:eventHistory>${events.map((e) => `<mcrs:event type="${esc(e.event_type)}" actorId="${esc(e.actor_id)}" occurredAt="${esc(e.occurred_at)}" />`).join('')}</mcrs:eventHistory>` : ''}
    </mcrs:record>`;
  });
  return `<?xml version="1.0" encoding="UTF-8"?>
<mcrs:export xmlns:mcrs="urn:moreq2010:mcrs">
  <mcrs:records>
    ${recordXml.join('\n')}
  </mcrs:records>
</mcrs:export>`;
}

function createRecordsExportRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '3mb' }));
  router.use(requireAuth);

  router.post('/export', async (req, res) => {
    try {
      const b = req.body || {};
      const scope = String(b.scope || '').trim();
      const scopeId = b.scopeId ? String(b.scopeId).trim() : '';
      const options = {
        includeComponents: !!b.includeComponents,
        includeEventHistory: !!b.includeEventHistory,
        includeAcl: !!b.includeAcl,
        includeDisposalSchedule: !!b.includeDisposalSchedule,
      };
      if (!['aggregation', 'classification', 'records'].includes(scope)) {
        return res.status(400).json({ error: 'scope không hợp lệ' });
      }

      let rows = [];
      if (scope === 'aggregation') {
        rows = db.prepare('SELECT * FROM rm_records WHERE aggregation_id = ?').all(scopeId);
      } else if (scope === 'classification') {
        rows = db.prepare('SELECT * FROM rm_records WHERE classification_id = ?').all(scopeId);
      } else {
        if (Array.isArray(scopeId)) {
          const ids = scopeId.map(String);
          const placeholders = ids.map(() => '?').join(',');
          rows = ids.length
            ? db.prepare(`SELECT * FROM rm_records WHERE id IN (${placeholders})`).all(...ids)
            : [];
        } else if (scopeId) {
          rows = db.prepare('SELECT * FROM rm_records WHERE id = ?').all(scopeId);
        } else {
          rows = db.prepare('SELECT * FROM rm_records ORDER BY capture_date DESC LIMIT 200').all();
        }
      }

      const xml = buildRecordsXml(rows, options, db);
      if (options.includeComponents) {
        const zip = new JSZip();
        const files = [];
        const manifestRecords = [];
        for (const r of rows) {
          const comps = db
            .prepare(`
              SELECT id, filename, stored_filename, mime_type, file_size, sha256_hash, version, component_label, uploaded_at
              FROM rm_components
              WHERE record_id = ?
              ORDER BY uploaded_at ASC
            `)
            .all(r.id);
          const componentManifest = [];
          for (const c of comps) {
            let abs = null;
            try {
              abs = resolveStoredPath(c.stored_filename || '');
            } catch (_e) {
              abs = null;
            }
            const exportName = `${r.id}/${c.stored_filename || c.filename || c.id}`;
            if (abs && fs.existsSync(abs)) {
              zip.file(`components/${exportName}`, fs.readFileSync(abs));
            }
            files.push({
              record_id: r.id,
              component_id: c.id,
              filename: c.filename,
              stored_filename: c.stored_filename,
              mime_type: c.mime_type,
              file_size: c.file_size,
              sha256_hash: c.sha256_hash,
              version: c.version,
              component_label: c.component_label,
              uploaded_at: c.uploaded_at,
              export_path: `components/${exportName}`,
              exists: !!(abs && fs.existsSync(abs)),
            });
            componentManifest.push(files[files.length - 1]);
          }
          manifestRecords.push({
            id: r.id,
            record_number: r.record_number,
            title: r.title,
            doc_type: r.doc_type,
            classification_id: r.classification_id,
            aggregation_id: r.aggregation_id,
            components: componentManifest,
          });
        }
        zip.file('records-export.xml', xml);
        zip.file('manifest.json', JSON.stringify({
          generated_at: new Date().toISOString(),
          scope,
          scopeId,
          options,
          records: manifestRecords,
          files,
        }, null, 2));
        const packed = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' });
        appendEvent(db, {
          event_type: 'export',
          entity_type: 'system',
          entity_id: 'records_export',
          actor_id: req.user.id,
          payload: { scope, scopeId, options, count: rows.length, format: 'zip' },
        });
        res.setHeader('Content-Type', 'application/zip');
        res.setHeader('Content-Disposition', `attachment; filename="records-export-${Date.now()}.zip"`);
        return res.send(packed);
      }
      appendEvent(db, {
        event_type: 'export',
        entity_type: 'system',
        entity_id: 'records_export',
        actor_id: req.user.id,
        payload: { scope, scopeId, options, count: rows.length, format: 'xml' },
      });
      res.setHeader('Content-Type', 'application/xml; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="records-export-${Date.now()}.xml"`);
      return res.send(xml);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi export records' });
    }
  });

  router.post('/import', upload.single('file'), async (req, res) => {
    try {
      if (!req.file) return res.status(400).json({ error: 'Thiếu file import' });
      let xml = '';
      const name = String(req.file.originalname || '').toLowerCase();
      if (name.endsWith('.zip')) {
        try {
          const raw = zlib.gunzipSync(req.file.buffer).toString('utf8');
          const parsed = JSON.parse(raw);
          xml = String(parsed.xml || '');
        } catch (_legacyErr) {
          const zip = await JSZip.loadAsync(req.file.buffer);
          if (zip.file('records-export.xml')) {
            xml = await zip.file('records-export.xml').async('string');
          } else if (zip.file('manifest.json')) {
            const manifest = JSON.parse(await zip.file('manifest.json').async('string'));
            xml = String(manifest.xml || '');
          }
        }
      } else {
        xml = req.file.buffer.toString('utf8');
      }
      if (!xml.includes('<mcrs:export') || !xml.includes('<mcrs:record')) {
        return res.status(400).json({ error: 'XML không hợp lệ hoặc thiếu element bắt buộc' });
      }
      const recordMatches = [...xml.matchAll(/<mcrs:record\b[^>]*>/g)];
      const aggregationRefs = [...xml.matchAll(/<mcrs:aggregationId>(.*?)<\/mcrs:aggregationId>/g)];
      const warnings = [];
      if (!xml.includes('<mcrs:triggerDate>')) warnings.push('Một số record có thể thiếu triggerDate');
      if (!xml.includes('<mcrs:retentionDueDate>')) warnings.push('Một số record có thể thiếu retentionDueDate');

      const previewId = crypto.randomUUID();
      importPreviewStore.set(previewId, { xml, createdBy: req.user.id, createdAt: Date.now() });
      return res.json({
        previewId,
        records: recordMatches.length,
        aggregations: aggregationRefs.length,
        warnings,
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi parse import file' });
    }
  });

  router.post('/import/confirm', (req, res) => {
    try {
      const previewId = String((req.body && req.body.previewId) || '').trim();
      if (!previewId) return res.status(400).json({ error: 'previewId là bắt buộc' });
      const preview = importPreviewStore.get(previewId);
      if (!preview) return res.status(404).json({ error: 'Preview không tồn tại hoặc đã hết hạn' });
      if (preview.createdBy !== req.user.id) {
        return res.status(403).json({ error: 'Không có quyền confirm preview này' });
      }
      const xml = preview.xml;
      const recordOpenTags = [...xml.matchAll(/<mcrs:record\b([^>]*)>/g)];

      const tx = db.transaction(() => {
        let inserted = 0;
        for (const m of recordOpenTags) {
          const attrs = m[1] || '';
          const idMatch = attrs.match(/id="([^"]+)"/);
          const numberMatch = attrs.match(/number="([^"]+)"/);
          const recId = idMatch ? idMatch[1] : crypto.randomUUID();
          const recNum = numberMatch ? numberMatch[1] : null;
          const blockStart = m.index;
          const blockEnd = xml.indexOf('</mcrs:record>', blockStart);
          if (blockEnd < 0) continue;
          const block = xml.slice(blockStart, blockEnd + '</mcrs:record>'.length);
          const getTag = (tag) => {
            const mm = block.match(new RegExp(`<mcrs:${tag}>([\\s\\S]*?)<\\/mcrs:${tag}>`));
            return mm ? mm[1] : null;
          };
          if (!getTag('title') || !getTag('docType')) continue;
          const existed = db.prepare('SELECT id FROM rm_records WHERE id = ?').get(recId);
          if (existed) continue;
          db.prepare(`
            INSERT INTO rm_records (
              id, record_number, title, doc_type, classification_id, aggregation_id,
              trigger_date, retention_due_date, state, is_frozen, capture_date, created_at, updated_at
            ) VALUES (
              @id, @record_number, @title, @doc_type, @classification_id, @aggregation_id,
              @trigger_date, @retention_due_date, 'active', 1, datetime('now'), datetime('now'), datetime('now')
            )
          `).run({
            id: recId,
            record_number: recNum || `IMP-${Date.now()}-${inserted + 1}`,
            title: getTag('title'),
            doc_type: getTag('docType'),
            classification_id: getTag('classificationId'),
            aggregation_id: getTag('aggregationId'),
            trigger_date: getTag('triggerDate'),
            retention_due_date: getTag('retentionDueDate'),
          });
          inserted += 1;
        }
        return inserted;
      });
      const insertedCount = tx();
      importPreviewStore.delete(previewId);
      appendEvent(db, {
        event_type: 'import',
        entity_type: 'system',
        entity_id: 'records_import',
        actor_id: req.user.id,
        payload: { inserted: insertedCount, previewId },
      });
      return res.json({ inserted: insertedCount });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi confirm import' });
    }
  });

  return router;
}

module.exports = createRecordsExportRouter;
