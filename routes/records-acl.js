'use strict';

const express = require('express');
const crypto = require('crypto');
const { appendEvent } = require('../migrations/0050_records_module.js');

const ALLOWED_PERMISSIONS = new Set(['view', 'download', 'edit', 'dispose', 'acl_manage']);

function checkEntityType(entityType) {
  return entityType === 'record' || entityType === 'aggregation';
}

function checkPrincipalType(principalType) {
  return principalType === 'user' || principalType === 'group';
}

function entityExists(db, entityType, entityId) {
  if (entityType === 'record') return !!db.prepare('SELECT id FROM rm_records WHERE id = ?').get(entityId);
  return !!db.prepare('SELECT id FROM rm_aggregations WHERE id = ?').get(entityId);
}

function createRecordsAclRouter(db, requireAuth) {
  const router = express.Router();
  router.use(express.json({ limit: '1mb' }));
  router.use(requireAuth);
  router.use(requireAuth.requirePermission('records.acl.manage'));

  router.get('/users', (_req, res) => {
    try {
      const rows = db
        .prepare(`
          SELECT
            u.id,
            u.name,
            u.email,
            r.name AS role_name,
            (SELECT COUNT(1) FROM rm_record_acl a WHERE a.principal_type = 'user' AND a.principal_id = u.id) AS entity_acl_count
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

  router.get('/groups', (_req, res) => {
    try {
      const rows = db
        .prepare(`
          SELECT
            g.id,
            g.name,
            g.description,
            (SELECT COUNT(1) FROM rm_record_acl a WHERE a.principal_type = 'group' AND a.principal_id = g.id) AS entity_acl_count
          FROM groups g
          ORDER BY g.name
        `)
        .all();
      return res.json(rows);
    } catch (_e) {
      return res.json([]);
    }
  });

  router.get('/:entityType/:entityId', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      if (!entityExists(db, entityType, entityId)) {
        return res.status(404).json({ error: `${entityType} không tồn tại` });
      }
      const rows = db
        .prepare(`
          SELECT
            a.*,
            CASE WHEN a.principal_type = 'user' THEN u.name ELSE g.name END AS principal_name,
            CASE WHEN a.principal_type = 'user' THEN u.email ELSE NULL END AS principal_email
          FROM rm_record_acl a
          LEFT JOIN users u ON a.principal_type = 'user' AND u.id = a.principal_id
          LEFT JOIN groups g ON a.principal_type = 'group' AND g.id = a.principal_id
          WHERE a.entity_type = ? AND a.entity_id = ?
          ORDER BY a.granted_at DESC
        `)
        .all(entityType, entityId);
      return res.json(rows);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi tải ACL entity' });
    }
  });

  router.post('/:entityType/:entityId/grants', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      if (!entityExists(db, entityType, entityId)) {
        return res.status(404).json({ error: `${entityType} không tồn tại` });
      }
      const body = req.body || {};
      const principalType = String(body.principalType || '').trim().toLowerCase();
      const principalId = String(body.principalId || body.userId || body.groupId || '').trim();
      const permission = String(body.permission || '').trim().toLowerCase();
      if (!principalType || !principalId || !permission) {
        return res.status(400).json({ error: 'principalType, principalId và permission là bắt buộc' });
      }
      if (!checkPrincipalType(principalType)) {
        return res.status(400).json({ error: 'principalType phải là user hoặc group' });
      }
      if (!ALLOWED_PERMISSIONS.has(permission)) {
        return res.status(400).json({ error: 'permission không hợp lệ' });
      }
      if (principalType === 'user') {
        const user = db.prepare('SELECT id FROM users WHERE id = ?').get(principalId);
        if (!user) return res.status(404).json({ error: 'User không tồn tại' });
      } else {
        const group = db.prepare('SELECT id FROM groups WHERE id = ?').get(principalId);
        if (!group) return res.status(404).json({ error: 'Group không tồn tại' });
      }

      const existed = db
        .prepare(`
          SELECT *
          FROM rm_record_acl
          WHERE entity_type = ? AND entity_id = ? AND principal_type = ? AND principal_id = ?
        `)
        .get(entityType, entityId, principalType, principalId);
      if (existed) {
        db.prepare(`
          UPDATE rm_record_acl
          SET permission = ?, granted_by = ?, granted_at = datetime('now')
          WHERE id = ?
        `).run(permission, req.user.id, existed.id);
      } else {
        db.prepare(`
          INSERT INTO rm_record_acl (
            id, entity_type, entity_id, principal_type, principal_id, permission, granted_by, granted_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        `).run(crypto.randomUUID(), entityType, entityId, principalType, principalId, permission, req.user.id);
      }
      const row = db
        .prepare(`
          SELECT
            a.*,
            CASE WHEN a.principal_type = 'user' THEN u.name ELSE g.name END AS principal_name,
            CASE WHEN a.principal_type = 'user' THEN u.email ELSE NULL END AS principal_email
          FROM rm_record_acl a
          LEFT JOIN users u ON a.principal_type = 'user' AND u.id = a.principal_id
          LEFT JOIN groups g ON a.principal_type = 'group' AND g.id = a.principal_id
          WHERE a.entity_type = ? AND a.entity_id = ? AND a.principal_type = ? AND a.principal_id = ?
        `)
        .get(entityType, entityId, principalType, principalId);
      appendEvent(db, {
        event_type: 'acl_change',
        entity_type: entityType,
        entity_id: entityId,
        actor_id: req.user.id,
        payload: {
          action: existed ? 'update' : 'grant',
          principalType,
          principalId,
          permission,
        },
      });
      return res.status(existed ? 200 : 201).json(row);
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi cấp quyền ACL' });
    }
  });

  router.delete('/:entityType/:entityId/grants/:grantId', (req, res) => {
    try {
      const entityType = String(req.params.entityType || '').trim();
      const entityId = String(req.params.entityId || '').trim();
      const grantId = String(req.params.grantId || '').trim();
      if (!checkEntityType(entityType)) {
        return res.status(400).json({ error: 'entityType phải là record hoặc aggregation' });
      }
      if (!grantId) {
        return res.status(400).json({ error: 'grantId là bắt buộc' });
      }
      const existed = db
        .prepare('SELECT * FROM rm_record_acl WHERE id = ? AND entity_type = ? AND entity_id = ?')
        .get(grantId, entityType, entityId);
      if (!existed) return res.status(404).json({ error: 'ACL không tồn tại' });
      db.prepare('DELETE FROM rm_record_acl WHERE id = ?').run(existed.id);
      appendEvent(db, {
        event_type: 'acl_change',
        entity_type: entityType,
        entity_id: entityId,
        actor_id: req.user.id,
        payload: {
          action: 'revoke',
          principalType: existed.principal_type,
          principalId: existed.principal_id,
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
