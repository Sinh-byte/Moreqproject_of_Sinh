'use strict';

const express = require('express');
const crypto = require('crypto');
const { appendEvent } = require('../migrations/0050_records_module.js');

function checkEntityType(entityType) {
  return entityType === 'record' || entityType === 'aggregation';
}

function createRecordsAclRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '1mb' }));
  router.use(requireAuth);

  router.get('/users', (_req, res) => {
    try {
      const rows = db
        .prepare(`
          SELECT
            u.id,
            u.name,
            u.email,
            r.name AS role_name,
            (SELECT COUNT(1) FROM rm_acl a WHERE a.user_id = u.id) AS entity_acl_count
          FROM users u
          LEFT JOIN roles r ON r.id = u.role_id
          ORDER BY u.name
        `)
        .all();
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải users ACL' });
    }
  });

  router.get('/:entityType/:entityId', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      const rows = db
        .prepare(`
          SELECT a.*, u.name AS user_name, u.email
          FROM rm_acl a
          LEFT JOIN users u ON u.id = a.user_id
          WHERE a.entity_type = ? AND a.entity_id = ?
          ORDER BY a.granted_at DESC
        `)
        .all(entityType, entityId);
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải ACL entity' });
    }
  });

  router.post('/:entityType/:entityId', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      const body = req.body || {};
      const userId = String(body.userId || '').trim();
      const permission = String(body.permission || '').trim();
      if (!userId || !permission) {
        return res.status(400).json({ error: 'userId và permission là bắt buộc' });
      }
      if (!['read', 'read_download', 'full_control'].includes(permission)) {
        return res.status(400).json({ error: 'permission không hợp lệ' });
      }
      const user = db.prepare('SELECT id, name FROM users WHERE id = ?').get(userId);
      if (!user) return res.status(404).json({ error: 'User không tồn tại' });

      const existed = db
        .prepare('SELECT * FROM rm_acl WHERE entity_type = ? AND entity_id = ? AND user_id = ?')
        .get(entityType, entityId, userId);
      if (existed) {
        db.prepare(`
          UPDATE rm_acl
          SET permission = ?, granted_by = ?, granted_at = datetime('now')
          WHERE id = ?
        `).run(permission, req.user.id, existed.id);
      } else {
        db.prepare(`
          INSERT INTO rm_acl (id, entity_type, entity_id, user_id, permission, granted_by, granted_at)
          VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        `).run(crypto.randomUUID(), entityType, entityId, userId, permission, req.user.id);
      }
      const row = db
        .prepare(`
          SELECT a.*, u.name AS user_name
          FROM rm_acl a
          LEFT JOIN users u ON u.id = a.user_id
          WHERE a.entity_type = ? AND a.entity_id = ? AND a.user_id = ?
        `)
        .get(entityType, entityId, userId);
      appendEvent(db, {
        event_type: 'acl_change',
        entity_type: entityType,
        entity_id: entityId,
        actor_id: req.user.id,
        payload: {
          action: existed ? 'update' : 'grant',
          userId,
          permission,
        },
      });
      return res.status(existed ? 200 : 201).json(row);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi cấp quyền ACL' });
    }
  });

  router.delete('/:entityType/:entityId/:userId', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      const userId = String(req.params.userId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      const existed = db
        .prepare('SELECT * FROM rm_acl WHERE entity_type = ? AND entity_id = ? AND user_id = ?')
        .get(entityType, entityId, userId);
      if (!existed) return res.status(404).json({ error: 'ACL không tồn tại' });
      db.prepare('DELETE FROM rm_acl WHERE id = ?').run(existed.id);
      appendEvent(db, {
        event_type: 'acl_change',
        entity_type: entityType,
        entity_id: entityId,
        actor_id: req.user.id,
        payload: {
          action: 'revoke',
          userId,
          permission: existed.permission,
        },
      });
      return res.json({ ok: true });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi thu hồi ACL' });
    }
  });

  return router;
}

module.exports = createRecordsAclRouter;
