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

/**
 * Minimal JWT middleware cho local/dev:
 * - Yêu cầu Authorization: Bearer <jwt>
 * - Decode payload để lấy userId/id + role
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
    req.user = {
      id: payload.userId || payload.id || null,
      role: payload.role || 'staff',
    };
    if (!req.user.id) {
      return res.status(401).json({ error: 'JWT thiếu userId/id' });
    }
    return next();
  } catch (_e) {
    return res.status(401).json({ error: 'Không decode được JWT payload' });
  }
}

app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Mount routes cụ thể trước, route generic /api/records đặt sau cùng.
app.use('/api/records/capture', createRecordsCaptureRouter(db, requireAuth));
app.use('/api/admin/records', createRecordsAdminRouter(db, requireAuth));
app.use('/api/records/disposal', createRecordsDisposalRouter(db, requireAuth));
app.use('/api/records/aggregations', createRecordsAggregationsRouter(db, requireAuth));
app.use('/api/records/audit', createRecordsAuditRouter(db, requireAuth));
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
