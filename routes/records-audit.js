'use strict';

const express = require('express');
const crypto = require('crypto');

function buildWhere(query) {
  const where = [];
  const params = {};
  if (query.event_type) {
    where.push('event_type = @event_type');
    params.event_type = String(query.event_type).trim();
  }
  if (query.entity_type) {
    where.push('entity_type = @entity_type');
    params.entity_type = String(query.entity_type).trim();
  }
  if (query.actor_id) {
    where.push('actor_id = @actor_id');
    params.actor_id = String(query.actor_id).trim();
  }
  if (query.from) {
    where.push('date(occurred_at) >= date(@from)');
    params.from = String(query.from).trim();
  }
  if (query.to) {
    where.push('date(occurred_at) <= date(@to)');
    params.to = String(query.to).trim();
  }
  return { whereSql: where.length ? `WHERE ${where.join(' AND ')}` : '', params };
}

function createRecordsAuditRouter(db, requireAuth) {
  const router = express.Router();
  router.use(requireAuth);

  router.get('/', (req, res) => {
    try {
      const page = Math.max(1, parseInt(req.query.page, 10) || 1);
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 50));
      const offset = (page - 1) * limit;
      const { whereSql, params } = buildWhere(req.query);

      const total = db
        .prepare(`SELECT COUNT(1) AS c FROM rm_event_history ${whereSql}`)
        .get(params).c;
      const rows = db
        .prepare(`
          SELECT id, event_type, entity_type, entity_id, entity_title, actor_id, actor_label,
                 before_state, after_state, payload, event_hash, prev_event_hash, occurred_at
          FROM rm_event_history
          ${whereSql}
          ORDER BY occurred_at DESC, id DESC
          LIMIT @limit OFFSET @offset
        `)
        .all({ ...params, limit, offset });
      return res.json({
        rows,
        total,
        page,
        totalPages: Math.max(1, Math.ceil(total / limit)),
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải audit logs' });
    }
  });

  router.get('/stats', (_req, res) => {
    try {
      const totalEvents = db.prepare('SELECT COUNT(1) AS c FROM rm_event_history').get().c;
      const byEventType = db
        .prepare(`
          SELECT event_type, COUNT(1) AS c
          FROM rm_event_history
          GROUP BY event_type
          ORDER BY c DESC
        `)
        .all();
      const byActor = db
        .prepare(`
          SELECT COALESCE(actor_label, actor_id, 'SYSTEM') AS actor, COUNT(1) AS c
          FROM rm_event_history
          GROUP BY COALESCE(actor_label, actor_id, 'SYSTEM')
          ORDER BY c DESC
          LIMIT 20
        `)
        .all();
      const last100 = db
        .prepare(`
          SELECT id, event_hash, prev_event_hash, occurred_at
          FROM rm_event_history
          ORDER BY occurred_at DESC, id DESC
          LIMIT 100
        `)
        .all();
      let integrityOk = true;
      const failures = [];
      for (let i = 0; i < last100.length - 1; i += 1) {
        const current = last100[i];
        const older = last100[i + 1];
        if (current.prev_event_hash !== older.event_hash) {
          integrityOk = false;
          failures.push({
            id: current.id,
            expected_prev: older.event_hash,
            actual_prev: current.prev_event_hash,
          });
        }
      }
      const disposalEvents7d = db
        .prepare(`
          SELECT COUNT(1) AS c
          FROM rm_event_history
          WHERE event_type LIKE 'disposal_%'
            AND datetime(occurred_at) >= datetime('now', '-7 days')
        `)
        .get().c;
      return res.json({
        total_events: totalEvents,
        by_event_type: byEventType,
        by_actor: byActor,
        disposal_events_7d: disposalEvents7d,
        integrity_check: {
          checked: last100.length,
          ok: integrityOk,
          failures,
        },
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi audit stats' });
    }
  });

  router.get('/verify', (req, res) => {
    try {
      const role = req.user && req.user.role ? String(req.user.role).toLowerCase() : '';
      const isAdmin = role === 'admin' || role === 'director';
      if (!isAdmin) {
        return res.status(403).json({ error: 'Chỉ admin/director được verify audit chain' });
      }
      const rows = db.prepare(`
        SELECT id, event_hash, prev_event_hash, occurred_at
        FROM rm_event_history
        ORDER BY occurred_at ASC, id ASC
      `).all();
      let ok = true;
      const failures = [];
      const seenHashes = new Set();
      let previousHash = null;
      for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i];
        const idx = i + 1;
        const expectedPrev = previousHash;
        const currentHash = String(row.event_hash || '');
        if (!/^[a-f0-9]{64}$/.test(currentHash)) {
          ok = false;
          failures.push({ index: idx, id: row.id, reason: 'invalid_hash_format', hash: currentHash });
        }
        if (seenHashes.has(currentHash)) {
          ok = false;
          failures.push({ index: idx, id: row.id, reason: 'duplicate_hash', hash: currentHash });
        }
        seenHashes.add(currentHash);
        if (row.prev_event_hash !== expectedPrev) {
          ok = false;
          failures.push({
            index: idx,
            id: row.id,
            reason: 'prev_hash_mismatch',
            expected_prev_event_hash: expectedPrev,
            actual_prev_event_hash: row.prev_event_hash,
          });
        }
        previousHash = currentHash || null;
      }
      // Fingerprint toàn bộ chuỗi để có bằng chứng snapshot lúc verify
      const snapshotFingerprint = crypto
        .createHash('sha256')
        .update(rows.map((x) => `${x.id}|${x.prev_event_hash || ''}|${x.event_hash || ''}|${x.occurred_at || ''}`).join('\n'))
        .digest('hex');
      return res.json({
        ok,
        checked: rows.length,
        failures,
        first_event_id: rows[0] ? rows[0].id : null,
        last_event_id: rows.length ? rows[rows.length - 1].id : null,
        chain_snapshot_fingerprint: snapshotFingerprint,
        verified_at: new Date().toISOString(),
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi verify audit chain' });
    }
  });

  router.get('/entity/:type/:id', (req, res) => {
    try {
      const type = String(req.params.type || '').trim();
      const id = String(req.params.id || '').trim();
      const rows = db
        .prepare(`
          SELECT *
          FROM rm_event_history
          WHERE entity_type = ? AND entity_id = ?
          ORDER BY occurred_at DESC, id DESC
        `)
        .all(type, id);
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi entity audit history' });
    }
  });

  return router;
}

module.exports = createRecordsAuditRouter;
