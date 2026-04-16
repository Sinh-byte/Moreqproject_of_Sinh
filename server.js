'use strict';

const path = require('path');
const express = require('express');
const Database = require('better-sqlite3');

const migration0050 = require('./migrations/0050_records_module.js');
const createRecordsCaptureRouter = require('./routes/records-capture.js');
const createRecordsRouter = require('./routes/records.js');
const createRecordsAdminRouter = require('./routes/records-admin.js');
const createRecordsDisposalRouter = require('./routes/records-disposal.js');
const createRecordsAggregationsRouter = require('./routes/records-aggregations.js');
const createRecordsAuditRouter = require('./routes/records-audit.js');
const createRecordsAclRouter = require('./routes/records-acl.js');
const createRecordsExportRouter = require('./routes/records-export.js');
const createRecordsDashboardRouter = require('./routes/records-dashboard.js');
const { startDisposalChecker } = require('./jobs/disposal-checker.js');

const PORT = 8080;
const DB_PATH = path.join(__dirname, 'data.sqlite');

const db = new Database(DB_PATH);
db.pragma('foreign_keys = ON');

// Tạo schema mới nếu chưa có.
// Nếu dự án thật đã có migration runner riêng thì có thể bỏ đoạn này.
try {
  migration0050.up(db);
} catch (e) {
  // Ignore when seed đã tồn tại hoặc schema đã sẵn.
  console.warn('[server] migration 0050 warning:', e.message);
}

const app = express();

app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: true }));

const ROLE_PERMISSION_MAP = {
  admin: ['*', 'view_confidential'],
  records_admin: ['records.view_all', 'records.view', 'records.acl.manage', 'view_confidential'],
  manager: ['records.view_all', 'records.view', 'records.export'],
  staff: ['records.view'],
};

const PERMISSION_ALIASES = {
  read: 'view',
  read_download: 'download',
  full_control: 'acl_manage',
};

function normalizePermission(value) {
  const raw = String(value || '').trim().toLowerCase();
  return PERMISSION_ALIASES[raw] || raw;
}

function permissionSetFromPayload(payload) {
  const set = new Set();
  const role = String(payload.role || 'staff').trim().toLowerCase();
  for (const p of ROLE_PERMISSION_MAP[role] || ROLE_PERMISSION_MAP.staff) set.add(p);
  const payloadPerms = Array.isArray(payload.permissions)
    ? payload.permissions
    : (typeof payload.permissions === 'string' ? payload.permissions.split(',') : []);
  for (const p of payloadPerms) {
    const normalized = normalizePermission(p);
    if (normalized) set.add(normalized);
  }
  return { role, permissions: [...set] };
}

function userPrincipalsFromReq(req) {
  const principals = [{ principal_type: 'user', principal_id: req.user.id }];
  if (Array.isArray(req.user.groups)) {
    for (const groupId of req.user.groups) {
      const gid = String(groupId || '').trim();
      if (gid) principals.push({ principal_type: 'group', principal_id: gid });
    }
  }
  return principals;
}

function hasPermission(req, permission) {
  const normalized = normalizePermission(permission);
  return req.user.permissions.includes('*') || req.user.permissions.includes(normalized);
}

function requirePermission(permission) {
  return (req, res, next) => {
    if (hasPermission(req, permission)) return next();
    return res.status(403).json({ error: `Thiếu quyền: ${permission}` });
  };
}

function makePrincipalPairCondition(principals, params, alias, prefix) {
  return principals
    .map((principal, idx) => {
      const typeKey = `${prefix}Type${idx}`;
      const idKey = `${prefix}Id${idx}`;
      params[typeKey] = principal.principal_type;
      params[idKey] = principal.principal_id;
      return `(${alias}.principal_type = @${typeKey} AND ${alias}.principal_id = @${idKey})`;
    })
    .join(' OR ');
}

function hasAclPermission(req, entityType, entityId, permission) {
  if (hasPermission(req, 'records.view_all')) return true;
  const normalizedPermission = normalizePermission(permission);
  const principals = userPrincipalsFromReq(req);
  if (!principals.length) return false;
  const params = {
    entityType: String(entityType || '').trim().toLowerCase(),
    entityId: String(entityId || '').trim(),
    permission: normalizedPermission,
  };
  const directPrincipalCond = makePrincipalPairCondition(principals, params, 'a', 'd');
  const inheritedPrincipalCond = makePrincipalPairCondition(principals, params, 'a', 'i');
  const directAcl = db.prepare(`
    SELECT 1
    FROM rm_record_acl a
    WHERE a.entity_type = @entityType
      AND a.entity_id = @entityId
      AND (${directPrincipalCond})
      AND (a.permission = @permission OR a.permission = 'acl_manage')
    LIMIT 1
  `).get(params);
  if (directAcl) return true;
  if (params.entityType !== 'record') return false;
  const inheritedAcl = db.prepare(`
    SELECT 1
    FROM rm_records r
    JOIN rm_record_acl a
      ON a.entity_type = 'aggregation'
     AND a.entity_id = r.aggregation_id
    WHERE r.id = @entityId
      AND (${inheritedPrincipalCond})
      AND (a.permission = @permission OR a.permission = 'acl_manage')
    LIMIT 1
  `).get(params);
  return !!inheritedAcl;
}

function ensureRbacTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS rm_record_acl (
      id TEXT PRIMARY KEY,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      principal_type TEXT NOT NULL,
      principal_id TEXT NOT NULL,
      permission TEXT NOT NULL,
      granted_by TEXT REFERENCES users(id),
      granted_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(entity_type, entity_id, principal_type, principal_id)
    );
    CREATE INDEX IF NOT EXISTS idx_rm_record_acl_entity
      ON rm_record_acl(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_rm_record_acl_principal
      ON rm_record_acl(principal_type, principal_id);
  `);
  const oldCount = db.prepare(`SELECT COUNT(1) AS c FROM rm_acl`).get().c;
  const newCount = db.prepare(`SELECT COUNT(1) AS c FROM rm_record_acl`).get().c;
  if (newCount === 0 && oldCount > 0) {
    db.exec(`
      INSERT INTO rm_record_acl (id, entity_type, entity_id, principal_type, principal_id, permission, granted_by, granted_at)
      SELECT id, entity_type, entity_id, 'user', user_id, permission, granted_by, granted_at
      FROM rm_acl
    `);
  }
}

function ensureSecurityColumns() {
  const cols = db.prepare(`PRAGMA table_info(rm_records)`).all();
  const hasConfidential = cols.some((c) => c.name === 'is_confidential');
  if (!hasConfidential) {
    db.exec(`
      ALTER TABLE rm_records
      ADD COLUMN is_confidential INTEGER NOT NULL DEFAULT 0
    `);
  }
}

/**
 * Minimal JWT middleware cho local/dev:
 * - Yêu cầu Authorization: Bearer <jwt>
 * - Decode payload để lấy userId/id + role + permissions + groups
 * - KHÔNG verify chữ ký (chỉ để chạy local nhanh)
 */
function requireAuth(req, res, next) {
  const auth = req.headers.authorization || '';
  if (!auth.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Thiếu Bearer token' });
  }
  const token = auth.slice(7).trim();
  const parts = token.split('.');
  if (parts.length < 2) {
    return res.status(401).json({ error: 'JWT không hợp lệ' });
  }
  try {
    const payloadJson = Buffer.from(parts[1], 'base64url').toString('utf8');
    const payload = JSON.parse(payloadJson);
    const resolved = permissionSetFromPayload(payload);
    req.user = {
      id: payload.userId || payload.id || null,
      role: resolved.role,
      permissions: resolved.permissions,
      groups: Array.isArray(payload.groups)
        ? payload.groups
        : (Array.isArray(payload.groupIds) ? payload.groupIds : []),
    };
    if (!req.user.id) {
      return res.status(401).json({ error: 'JWT thiếu userId/id' });
    }
    return next();
  } catch (_e) {
    return res.status(401).json({ error: 'Không decode được JWT payload' });
  }
}
requireAuth.requirePermission = requirePermission;
requireAuth.hasPermission = hasPermission;
requireAuth.hasAclPermission = hasAclPermission;
requireAuth.userPrincipalsFromReq = userPrincipalsFromReq;

ensureRbacTables();
ensureSecurityColumns();

app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Mount routes cụ thể trước, route generic /api/records đặt sau cùng.
app.use('/api/records/capture', createRecordsCaptureRouter(db, requireAuth));
app.use('/api/admin/records', createRecordsAdminRouter(db, requireAuth));
app.use('/api/records/disposal', createRecordsDisposalRouter(db, requireAuth));
app.use('/api/records/aggregations', createRecordsAggregationsRouter(db, requireAuth));
app.use('/api/records/audit', createRecordsAuditRouter(db, requireAuth));
app.use('/api/admin/audit', createRecordsAuditRouter(db, requireAuth));
app.use('/api/records/acl', createRecordsAclRouter(db, requireAuth));
app.use('/api/records', createRecordsExportRouter(db, requireAuth));
app.use('/api/records/dashboard', createRecordsDashboardRouter(db, requireAuth));
app.use('/api/records', createRecordsRouter(db, requireAuth));

app.get('/', (_req, res) => {
  res.redirect('/records/index.html');
});

startDisposalChecker(db);

app.listen(PORT, () => {
  console.log(`[server] Running at http://localhost:${PORT}`);
});
