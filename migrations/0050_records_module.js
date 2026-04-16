'use strict';

const crypto = require('crypto');

/**
 * MoReq2010 — Quản lý Văn bản & Hồ sơ
 * Migration: tạo bảng, index, seed, export generateRecordNumber + appendEvent
 */

function up(db) {
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS rm_disposal_schedules (
      id TEXT PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      legal_basis TEXT,
      trigger_event TEXT NOT NULL,
      retention_years INTEGER,
      is_permanent INTEGER NOT NULL DEFAULT 0,
      disposal_action TEXT NOT NULL,
      review_action_approve TEXT,
      review_action_retain TEXT,
      confirmation_days INTEGER NOT NULL DEFAULT 30,
      alert_recipients TEXT,
      notes TEXT,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_by TEXT REFERENCES users(id),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_classifications (
      id TEXT PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      description TEXT,
      parent_id TEXT REFERENCES rm_classifications(id),
      disposal_schedule_id TEXT REFERENCES rm_disposal_schedules(id),
      inherit_disposal INTEGER NOT NULL DEFAULT 1,
      is_open INTEGER NOT NULL DEFAULT 1,
      sort_order INTEGER DEFAULT 0,
      created_by TEXT REFERENCES users(id),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_aggregations (
      id TEXT PRIMARY KEY,
      code TEXT UNIQUE NOT NULL,
      title TEXT NOT NULL,
      agg_type TEXT NOT NULL,
      parent_id TEXT REFERENCES rm_aggregations(id),
      classification_id TEXT REFERENCES rm_classifications(id),
      disposal_schedule_id TEXT REFERENCES rm_disposal_schedules(id),
      is_open INTEGER NOT NULL DEFAULT 1,
      manager_id TEXT REFERENCES users(id),
      stims_project_id TEXT,
      notes TEXT,
      created_by TEXT REFERENCES users(id),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_records (
      id TEXT PRIMARY KEY,
      record_number TEXT UNIQUE NOT NULL,
      doc_ref TEXT,
      title TEXT NOT NULL,
      doc_type TEXT NOT NULL,
      classification_id TEXT REFERENCES rm_classifications(id),
      aggregation_id TEXT REFERENCES rm_aggregations(id),
      disposal_schedule_id TEXT REFERENCES rm_disposal_schedules(id),
      issue_date TEXT,
      effective_date TEXT,
      expiry_date TEXT,
      trigger_date TEXT,
      retention_due_date TEXT,
      author TEXT,
      approver TEXT,
      issuing_unit TEXT,
      captured_by TEXT REFERENCES users(id),
      capture_date TEXT NOT NULL DEFAULT (datetime('now')),
      description TEXT,
      keywords TEXT,
      state TEXT NOT NULL DEFAULT 'draft',
      is_frozen INTEGER NOT NULL DEFAULT 0,
      is_vital INTEGER NOT NULL DEFAULT 0,
      disposal_alert_sent_at TEXT,
      disposal_confirmed_at TEXT,
      disposal_confirmed_by TEXT REFERENCES users(id),
      disposal_action_taken TEXT,
      disposal_notes TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_components (
      id TEXT PRIMARY KEY,
      record_id TEXT NOT NULL REFERENCES rm_records(id) ON DELETE CASCADE,
      filename TEXT NOT NULL,
      stored_filename TEXT NOT NULL,
      mime_type TEXT NOT NULL,
      file_size INTEGER NOT NULL,
      sha256_hash TEXT NOT NULL,
      version TEXT NOT NULL DEFAULT '1.0',
      is_primary INTEGER NOT NULL DEFAULT 0,
      component_label TEXT,
      uploaded_by TEXT REFERENCES users(id),
      uploaded_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_event_history (
      id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      entity_title TEXT,
      actor_id TEXT REFERENCES users(id),
      actor_label TEXT,
      ip_address TEXT,
      session_id TEXT,
      before_state TEXT,
      after_state TEXT,
      payload TEXT,
      event_hash TEXT NOT NULL,
      prev_event_hash TEXT,
      occurred_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_acl (
      id TEXT PRIMARY KEY,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      permission TEXT NOT NULL,
      granted_by TEXT REFERENCES users(id),
      granted_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(entity_type, entity_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS rm_disposal_workflow (
      id TEXT PRIMARY KEY,
      record_id TEXT NOT NULL UNIQUE REFERENCES rm_records(id),
      triggered_at TEXT NOT NULL DEFAULT (datetime('now')),
      deadline TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      proposed_action TEXT,
      proposed_by TEXT REFERENCES users(id),
      proposed_at TEXT,
      proposal_notes TEXT,
      approved_action TEXT,
      approved_by TEXT REFERENCES users(id),
      approved_at TEXT,
      approval_notes TEXT,
      completed_at TEXT
    );

    CREATE TABLE IF NOT EXISTS rm_tags (
      id TEXT PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      color_class TEXT DEFAULT 'teal',
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS rm_record_tags (
      record_id TEXT NOT NULL REFERENCES rm_records(id) ON DELETE CASCADE,
      tag_id TEXT NOT NULL REFERENCES rm_tags(id) ON DELETE CASCADE,
      PRIMARY KEY (record_id, tag_id)
    );

    CREATE INDEX IF NOT EXISTS idx_rm_records_state ON rm_records(state);
    CREATE INDEX IF NOT EXISTS idx_rm_records_classification_id ON rm_records(classification_id);
    CREATE INDEX IF NOT EXISTS idx_rm_records_aggregation_id ON rm_records(aggregation_id);
    CREATE INDEX IF NOT EXISTS idx_rm_records_retention_due_date ON rm_records(retention_due_date);
    CREATE INDEX IF NOT EXISTS idx_rm_records_capture_date ON rm_records(capture_date DESC);

    CREATE INDEX IF NOT EXISTS idx_rm_event_history_entity ON rm_event_history(entity_type, entity_id);
    CREATE INDEX IF NOT EXISTS idx_rm_event_history_actor_id ON rm_event_history(actor_id);
    CREATE INDEX IF NOT EXISTS idx_rm_event_history_occurred_at ON rm_event_history(occurred_at DESC);
    CREATE INDEX IF NOT EXISTS idx_rm_event_history_event_type ON rm_event_history(event_type);

    CREATE INDEX IF NOT EXISTS idx_rm_classifications_parent_id ON rm_classifications(parent_id);
    CREATE INDEX IF NOT EXISTS idx_rm_aggregations_parent_id ON rm_aggregations(parent_id);
    CREATE INDEX IF NOT EXISTS idx_rm_aggregations_stims_project_id ON rm_aggregations(stims_project_id);

    CREATE INDEX IF NOT EXISTS idx_rm_acl_user_id ON rm_acl(user_id);
    CREATE INDEX IF NOT EXISTS idx_rm_acl_entity ON rm_acl(entity_type, entity_id);
  `);

  const tableNames = [
    'rm_disposal_schedules',
    'rm_classifications',
    'rm_aggregations',
    'rm_records',
    'rm_components',
    'rm_event_history',
    'rm_acl',
    'rm_disposal_workflow',
    'rm_tags',
    'rm_record_tags',
  ];

  let seedRows = 0;

  const alreadySeeded = db
    .prepare(
      `SELECT 1 AS x FROM rm_disposal_schedules WHERE code = 'DS-NOIDBO-10Y' LIMIT 1`
    )
    .get();
  if (alreadySeeded) {
    console.log(
      `[0050_records_module] Đã tạo ${tableNames.length} bảng; seed mẫu đã có (bỏ qua insert).`
    );
    return { tablesCreated: tableNames.length, seedRowsInserted: 0 };
  }

  const insDs = db.prepare(`
    INSERT INTO rm_disposal_schedules (
      id, code, name, legal_basis, trigger_event, retention_years, is_permanent,
      disposal_action, review_action_approve, review_action_retain, confirmation_days,
      alert_recipients, notes, is_active, created_by
    ) VALUES (
      @id, @code, @name, @legal_basis, @trigger_event, @retention_years, @is_permanent,
      @disposal_action, @review_action_approve, @review_action_retain, @confirmation_days,
      @alert_recipients, @notes, @is_active, @created_by
    )
  `);

  const dsIds = {
    noidbo: crypto.randomUUID(),
    htc: crypto.randomUUID(),
    deta: crypto.randomUUID(),
    perm: crypto.randomUUID(),
    bienban: crypto.randomUUID(),
  };

  const disposalSeeds = [
    {
      id: dsIds.noidbo,
      code: 'DS-NOIDBO-10Y',
      name: 'Văn bản nội bộ — lưu 10 năm kể từ hết hiệu lực',
      legal_basis: 'Quy định nội bộ lưu trữ',
      trigger_event: 'expiry_date',
      retention_years: 10,
      is_permanent: 0,
      disposal_action: 'destruction',
      review_action_approve: null,
      review_action_retain: null,
      confirmation_days: 30,
      alert_recipients: null,
      notes: null,
      is_active: 1,
      created_by: null,
    },
    {
      id: dsIds.htc,
      code: 'DS-HTC-20Y',
      name: 'Hợp đồng — 20 năm sau kết thúc hợp đồng',
      legal_basis: null,
      trigger_event: 'closure_date',
      retention_years: 20,
      is_permanent: 0,
      disposal_action: 'review',
      review_action_approve: 'destruction',
      review_action_retain: 'extend_5y',
      confirmation_days: 30,
      alert_recipients: null,
      notes: null,
      is_active: 1,
      created_by: null,
    },
    {
      id: dsIds.deta,
      code: 'DS-DETA-15Y',
      name: 'Hồ sơ đề tài — 15 năm sau nghiệm thu',
      legal_basis: null,
      trigger_event: 'custom',
      retention_years: 15,
      is_permanent: 0,
      disposal_action: 'preservation',
      review_action_approve: null,
      review_action_retain: null,
      confirmation_days: 30,
      alert_recipients: null,
      notes: 'Kích hoạt theo ngày nghiệm thu (trigger_date)',
      is_active: 1,
      created_by: null,
    },
    {
      id: dsIds.perm,
      code: 'DS-TSTT-PERM',
      name: 'Tài sản tri thức — lưu vĩnh viễn',
      legal_basis: null,
      trigger_event: 'issue_date',
      retention_years: null,
      is_permanent: 1,
      disposal_action: 'preservation',
      review_action_approve: null,
      review_action_retain: null,
      confirmation_days: 30,
      alert_recipients: null,
      notes: null,
      is_active: 1,
      created_by: null,
    },
    {
      id: dsIds.bienban,
      code: 'DS-BIENBAN-5Y',
      name: 'Biên bản — 5 năm kể từ ngày lập',
      legal_basis: null,
      trigger_event: 'issue_date',
      retention_years: 5,
      is_permanent: 0,
      disposal_action: 'destruction',
      review_action_approve: null,
      review_action_retain: null,
      confirmation_days: 30,
      alert_recipients: null,
      notes: null,
      is_active: 1,
      created_by: null,
    },
  ];

  for (const row of disposalSeeds) {
    insDs.run(row);
    seedRows += 1;
  }

  const insCl = db.prepare(`
    INSERT INTO rm_classifications (
      id, code, name, description, parent_id, disposal_schedule_id,
      inherit_disposal, is_open, sort_order, created_by
    ) VALUES (
      @id, @code, @name, @description, @parent_id, @disposal_schedule_id,
      @inherit_disposal, @is_open, @sort_order, @created_by
    )
  `);

  const clIds = {
    sci: crypto.randomUUID(),
    vb: crypto.randomUUID(),
    vbQc: crypto.randomUUID(),
    vbQd: crypto.randomUUID(),
    hsDt: crypto.randomUUID(),
    hd: crypto.randomUUID(),
    hdQt: crypto.randomUUID(),
    hdDe: crypto.randomUUID(),
  };

  const clSeeds = [
    {
      id: clIds.sci,
      code: 'SCI',
      name: 'Khoa học công nghệ',
      description: 'Gốc phân loại nghiệp vụ',
      parent_id: null,
      disposal_schedule_id: dsIds.perm,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 0,
      created_by: null,
    },
    {
      id: clIds.vb,
      code: 'SCI.VB',
      name: 'Văn bản pháp lý nội bộ',
      description: null,
      parent_id: clIds.sci,
      disposal_schedule_id: dsIds.noidbo,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 10,
      created_by: null,
    },
    {
      id: clIds.vbQc,
      code: 'SCI.VB.QC',
      name: 'Quy chế & Quy định',
      description: null,
      parent_id: clIds.vb,
      disposal_schedule_id: null,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 1,
      created_by: null,
    },
    {
      id: clIds.vbQd,
      code: 'SCI.VB.QD',
      name: 'Quyết định hành chính',
      description: null,
      parent_id: clIds.vb,
      disposal_schedule_id: null,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 2,
      created_by: null,
    },
    {
      id: clIds.hsDt,
      code: 'SCI.HS.DT',
      name: 'Hồ sơ đề tài KHCN',
      description: null,
      parent_id: clIds.sci,
      disposal_schedule_id: dsIds.deta,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 20,
      created_by: null,
    },
    {
      id: clIds.hd,
      code: 'SCI.HT.TT',
      name: 'Hợp đồng & Thỏa thuận',
      description: null,
      parent_id: clIds.sci,
      disposal_schedule_id: dsIds.htc,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 30,
      created_by: null,
    },
    {
      id: clIds.hdQt,
      code: 'SCI.HT.HTCQT',
      name: 'Hợp đồng quốc tế',
      description: null,
      parent_id: clIds.hd,
      disposal_schedule_id: null,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 1,
      created_by: null,
    },
    {
      id: clIds.hdDe,
      code: 'SCI.HT.HDDE',
      name: 'Hợp đồng đề tài',
      description: null,
      parent_id: clIds.hd,
      disposal_schedule_id: null,
      inherit_disposal: 1,
      is_open: 1,
      sort_order: 2,
      created_by: null,
    },
  ];

  for (const row of clSeeds) {
    insCl.run(row);
    seedRows += 1;
  }

  const insAgg = db.prepare(`
    INSERT INTO rm_aggregations (
      id, code, title, agg_type, parent_id, classification_id, disposal_schedule_id,
      is_open, manager_id, stims_project_id, notes, created_by
    ) VALUES (
      @id, @code, @title, @agg_type, @parent_id, @classification_id, @disposal_schedule_id,
      @is_open, @manager_id, @stims_project_id, @notes, @created_by
    )
  `);

  const aggIds = {
    nk: crypto.randomUUID(),
    clinostat: crypto.randomUUID(),
  };

  insAgg.run({
    id: aggIds.nk,
    code: 'AGG-2024-008',
    title: 'NK Cell 2024',
    agg_type: 'research_project',
    parent_id: null,
    classification_id: clIds.hsDt,
    disposal_schedule_id: null,
    is_open: 1,
    manager_id: null,
    stims_project_id: null,
    notes: null,
    created_by: null,
  });
  seedRows += 1;

  insAgg.run({
    id: aggIds.clinostat,
    code: 'AGG-2023-005',
    title: 'Clinostat SMSG',
    agg_type: 'research_project',
    parent_id: null,
    classification_id: clIds.hsDt,
    disposal_schedule_id: null,
    is_open: 1,
    manager_id: null,
    stims_project_id: null,
    notes: null,
    created_by: null,
  });
  seedRows += 1;

  const insTag = db.prepare(`
    INSERT INTO rm_tags (id, name, color_class) VALUES (@id, @name, @color_class)
  `);

  const tagIds = {
    khcn: crypto.randomUUID(),
    tstt: crypto.randomUUID(),
    crd: crypto.randomUUID(),
  };

  insTag.run({ id: tagIds.khcn, name: 'KHCN', color_class: 'teal' });
  insTag.run({ id: tagIds.tstt, name: 'TSTT', color_class: 'blue' });
  insTag.run({ id: tagIds.crd, name: 'CRD Lab', color_class: 'purple' });
  seedRows += 3;

  const insRec = db.prepare(`
    INSERT INTO rm_records (
      id, record_number, doc_ref, title, doc_type, classification_id, aggregation_id,
      disposal_schedule_id, issue_date, effective_date, expiry_date, trigger_date,
      retention_due_date, author, approver, issuing_unit, captured_by, capture_date,
      description, keywords, state, is_frozen, is_vital,
      disposal_alert_sent_at, disposal_confirmed_at, disposal_confirmed_by,
      disposal_action_taken, disposal_notes
    ) VALUES (
      @id, @record_number, @doc_ref, @title, @doc_type, @classification_id, @aggregation_id,
      @disposal_schedule_id, @issue_date, @effective_date, @expiry_date, @trigger_date,
      @retention_due_date, @author, @approver, @issuing_unit, @captured_by, @capture_date,
      @description, @keywords, @state, @is_frozen, @is_vital,
      @disposal_alert_sent_at, @disposal_confirmed_at, @disposal_confirmed_by,
      @disposal_action_taken, @disposal_notes
    )
  `);

  const recIds = {
    r1: crypto.randomUUID(),
    r2: crypto.randomUUID(),
  };

  const year = new Date().getFullYear();
  const recNum1 = `REC-${year}-0001`;
  const recNum2 = `REC-${year}-0002`;

  insRec.run({
    id: recIds.r1,
    record_number: recNum1,
    doc_ref: `QC-KHCN-01/${year}`,
    title: 'Quy chế quản lý hồ sơ đề tài (mẫu seed)',
    doc_type: 'regulation',
    classification_id: clIds.vbQc,
    aggregation_id: null,
    disposal_schedule_id: null,
    issue_date: `${year}-01-15`,
    effective_date: `${year}-02-01`,
    expiry_date: null,
    trigger_date: `${year}-02-01`,
    retention_due_date: null,
    author: 'Ban hành nội bộ',
    approver: null,
    issuing_unit: 'Phòng KHCN',
    captured_by: null,
    capture_date: new Date().toISOString(),
    description: 'Bản ghi seed để test UI (active, frozen)',
    keywords: 'quy chế,hồ sơ',
    state: 'active',
    is_frozen: 1,
    is_vital: 0,
    disposal_alert_sent_at: null,
    disposal_confirmed_at: null,
    disposal_confirmed_by: null,
    disposal_action_taken: null,
    disposal_notes: null,
  });
  seedRows += 1;

  insRec.run({
    id: recIds.r2,
    record_number: recNum2,
    doc_ref: `BB-KHCN-12/${year}`,
    title: 'Biên bản nghiệm thu giai đoạn (mẫu seed)',
    doc_type: 'minutes',
    classification_id: clIds.hsDt,
    aggregation_id: aggIds.nk,
    disposal_schedule_id: null,
    issue_date: `${year}-06-01`,
    effective_date: null,
    expiry_date: null,
    trigger_date: `${year}-06-01`,
    retention_due_date: `${year + 5}-06-01`,
    author: 'Tổ nghiệm thu',
    approver: null,
    issuing_unit: 'Đề tài NK Cell 2024',
    captured_by: null,
    capture_date: new Date().toISOString(),
    description: 'Bản ghi seed thứ hai — gắn aggregation',
    keywords: 'nghiệm thu,biên bản',
    state: 'active',
    is_frozen: 1,
    is_vital: 1,
    disposal_alert_sent_at: null,
    disposal_confirmed_at: null,
    disposal_confirmed_by: null,
    disposal_action_taken: null,
    disposal_notes: null,
  });
  seedRows += 1;

  const insRt = db.prepare(`
    INSERT INTO rm_record_tags (record_id, tag_id) VALUES (@record_id, @tag_id)
  `);
  insRt.run({ record_id: recIds.r1, tag_id: tagIds.khcn });
  insRt.run({ record_id: recIds.r2, tag_id: tagIds.tstt });
  insRt.run({ record_id: recIds.r2, tag_id: tagIds.crd });
  seedRows += 3;

  console.log(
    `[0050_records_module] Đã tạo ${tableNames.length} bảng; đã insert ${seedRows} dòng seed (disposal + classifications + aggregations + tags + records + record_tags).`
  );

  return { tablesCreated: tableNames.length, seedRowsInserted: seedRows };
}

/**
 * Sinh số hiệu REC-YYYY-NNNN (tăng theo năm hiện tại, 4 chữ số).
 */
function generateRecordNumber(db) {
  const year = new Date().getFullYear();
  const prefix = `REC-${year}-`;
  const rows = db
    .prepare(
      `SELECT record_number FROM rm_records WHERE record_number LIKE ?`
    )
    .all(`${prefix}%`);

  let maxSeq = 0;
  const re = new RegExp(`^REC-${year}-(\\d+)$`);
  for (const r of rows) {
    const m = r.record_number.match(re);
    if (m) {
      const n = parseInt(m[1], 10);
      if (!Number.isNaN(n)) maxSeq = Math.max(maxSeq, n);
    }
  }

  const next = maxSeq + 1;
  if (next > 9999) {
    throw new Error(`generateRecordNumber: đã vượt giới hạn 9999 cho năm ${year}`);
  }
  return `${prefix}${String(next).padStart(4, '0')}`;
}

/**
 * Ghi nhận sự kiện audit (append-only).
 * event_hash = SHA256(prev_event_hash + JSON.stringify(eventData)) — prev rỗng nếu chưa có bản ghi trước.
 */
function _toStoredText(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function appendEvent(db, eventData) {
  if (!eventData || typeof eventData !== 'object') {
    throw new TypeError('appendEvent: eventData phải là object');
  }

  const prevRow = db
    .prepare(
      `SELECT event_hash FROM rm_event_history ORDER BY occurred_at DESC, id DESC LIMIT 1`
    )
    .get();

  const prevChain = prevRow ? prevRow.event_hash : '';
  const serialized = JSON.stringify(eventData);
  const event_hash = crypto
    .createHash('sha256')
    .update(prevChain + serialized)
    .digest('hex');

  const id = crypto.randomUUID();
  const prev_event_hash = prevRow ? prevChain : null;

  const stmt = db.prepare(`
    INSERT INTO rm_event_history (
      id, event_type, entity_type, entity_id, entity_title,
      actor_id, actor_label, ip_address, session_id,
      before_state, after_state, payload,
      event_hash, prev_event_hash, occurred_at
    ) VALUES (
      @id, @event_type, @entity_type, @entity_id, @entity_title,
      @actor_id, @actor_label, @ip_address, @session_id,
      @before_state, @after_state, @payload,
      @event_hash, @prev_event_hash, COALESCE(@occurred_at, datetime('now'))
    )
  `);

  stmt.run({
    id,
    event_type: eventData.event_type,
    entity_type: eventData.entity_type,
    entity_id: eventData.entity_id,
    entity_title: eventData.entity_title ?? null,
    actor_id: eventData.actor_id ?? null,
    actor_label: eventData.actor_label ?? null,
    ip_address: eventData.ip_address ?? null,
    session_id: eventData.session_id ?? null,
    before_state: _toStoredText(eventData.before_state),
    after_state: _toStoredText(eventData.after_state),
    payload: _toStoredText(eventData.payload),
    event_hash,
    prev_event_hash,
    occurred_at: eventData.occurred_at ?? null,
  });

  return { id, event_hash, prev_event_hash };
}

function _runAppendEventSelfTest() {
  const Database = require('better-sqlite3');
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE users (id TEXT PRIMARY KEY);
    INSERT INTO users (id) VALUES ('u-test');
    CREATE TABLE rm_event_history (
      id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id TEXT NOT NULL,
      entity_title TEXT,
      actor_id TEXT REFERENCES users(id),
      actor_label TEXT,
      ip_address TEXT,
      session_id TEXT,
      before_state TEXT,
      after_state TEXT,
      payload TEXT,
      event_hash TEXT NOT NULL,
      prev_event_hash TEXT,
      occurred_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
  `);

  const e1 = {
    event_type: 'system',
    entity_type: 'system',
    entity_id: 'boot',
    actor_id: 'u-test',
  };
  const r1 = appendEvent(db, e1);
  const serialized1 = JSON.stringify(e1);
  const expect1 = crypto
    .createHash('sha256')
    .update('' + serialized1)
    .digest('hex');
  if (r1.event_hash !== expect1) {
    throw new Error('appendEvent self-test: hash event đầu không khớp');
  }
  if (r1.prev_event_hash !== null) {
    throw new Error('appendEvent self-test: prev_event_hash đầu phải NULL');
  }

  const e2 = {
    event_type: 'capture',
    entity_type: 'record',
    entity_id: 'rec-1',
    payload: '{"x":1}',
  };
  const r2 = appendEvent(db, e2);
  const serialized2 = JSON.stringify(e2);
  const expect2 = crypto
    .createHash('sha256')
    .update(r1.event_hash + serialized2)
    .digest('hex');
  if (r2.event_hash !== expect2) {
    throw new Error('appendEvent self-test: chuỗi hash event thứ hai không khớp');
  }
  if (r2.prev_event_hash !== r1.event_hash) {
    throw new Error('appendEvent self-test: prev_event_hash thứ hai sai');
  }

  const rows = db.prepare(`SELECT COUNT(*) AS c FROM rm_event_history`).get();
  if (rows.c !== 2) {
    throw new Error('appendEvent self-test: số dòng không đúng');
  }

  db.close();
  console.log('[0050_records_module] appendEvent self-test: OK');
}

if (require.main === module) {
  _runAppendEventSelfTest();
}

module.exports = {
  up,
  generateRecordNumber,
  appendEvent,
};
