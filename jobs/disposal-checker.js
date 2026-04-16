'use strict';

const { appendEvent } = require('../migrations/0050_records_module.js');
const crypto = require('crypto');

let cronLib = null;
try {
  // Optional dependency; fallback to setInterval when unavailable.
  // eslint-disable-next-line global-require
  cronLib = require('node-cron');
} catch (_e) {
  cronLib = null;
}

function addDaysIsoDateTime(days) {
  const dt = new Date();
  dt.setUTCDate(dt.getUTCDate() + days);
  return dt.toISOString().slice(0, 19).replace('T', ' ');
}

function runDisposalCheck(db) {
  const dueRecords = db.prepare(`
    SELECT
      r.id,
      r.title,
      r.is_frozen,
      r.disposal_schedule_id,
      IFNULL(ds.confirmation_days, 30) AS confirmation_days
    FROM rm_records r
    LEFT JOIN rm_disposal_schedules ds ON ds.id = r.disposal_schedule_id
    WHERE r.state IN ('active', 'review')
      AND r.retention_due_date IS NOT NULL
      AND date(r.retention_due_date) <= date('now')
      AND r.disposal_alert_sent_at IS NULL
  `).all();

  if (!dueRecords.length) {
    console.log('[disposal-checker] Không có record đến hạn.');
    return { triggered: 0 };
  }

  const nowSql = "datetime('now')";
  const insertWorkflow = db.prepare(`
    INSERT INTO rm_disposal_workflow (
      id, record_id, triggered_at, deadline, status
    ) VALUES (
      @id, @record_id, datetime('now'), @deadline, @status
    )
  `);
  const updateRecord = db.prepare(`
    UPDATE rm_records
    SET disposal_alert_sent_at = ${nowSql},
        state = 'review',
        updated_at = ${nowSql}
    WHERE id = @record_id
  `);
  const holdWorkflow = db.prepare(`
    UPDATE rm_disposal_workflow
    SET status = 'on_hold'
    WHERE record_id = @record_id
      AND status <> 'completed'
  `);

  const tx = db.transaction((rows) => {
    let inserted = 0;
    let onHold = 0;
    for (const r of rows) {
      const exists = db
        .prepare('SELECT id FROM rm_disposal_workflow WHERE record_id = ?')
        .get(r.id);
      if (r.is_frozen === 1) {
        if (!exists) {
          insertWorkflow.run({
            id: crypto.randomUUID(),
            record_id: r.id,
            deadline: addDaysIsoDateTime(r.confirmation_days || 30),
            status: 'on_hold',
          });
          appendEvent(db, {
            event_type: 'disposal_hold',
            entity_type: 'record',
            entity_id: r.id,
            entity_title: r.title,
            actor_id: null,
            payload: { source: 'daily_checker', reason: 'record_frozen' },
          });
          onHold += 1;
        } else {
          holdWorkflow.run({ record_id: r.id });
        }
        continue;
      }
      if (exists) continue;

      insertWorkflow.run({
        id: crypto.randomUUID(),
        record_id: r.id,
        deadline: addDaysIsoDateTime(r.confirmation_days || 30),
        status: 'pending_review',
      });
      updateRecord.run({ record_id: r.id });
      appendEvent(db, {
        event_type: 'disposal_alert',
        entity_type: 'record',
        entity_id: r.id,
        entity_title: r.title,
        actor_id: null,
        payload: {
          source: 'daily_checker',
          confirmation_days: r.confirmation_days || 30,
        },
      });
      inserted += 1;
    }
    return { inserted, onHold };
  });

  const outcome = tx(dueRecords);
  console.log(`[disposal-checker] Đã trigger ${outcome.inserted} record(s), on_hold ${outcome.onHold}.`);
  return { triggered: outcome.inserted, onHold: outcome.onHold };
}

function startDisposalChecker(db) {
  if (cronLib && typeof cronLib.schedule === 'function') {
    const task = cronLib.schedule('0 9 * * *', () => {
      try {
        runDisposalCheck(db);
      } catch (e) {
        console.error('[disposal-checker] Cron error:', e.message);
      }
    });
    console.log('[disposal-checker] Started with node-cron: 0 9 * * *');
    return task;
  }

  const oneDayMs = 24 * 60 * 60 * 1000;
  const id = setInterval(() => {
    const now = new Date();
    if (now.getHours() === 9 && now.getMinutes() === 0) {
      try {
        runDisposalCheck(db);
      } catch (e) {
        console.error('[disposal-checker] Interval error:', e.message);
      }
    }
  }, 60 * 1000);
  console.log('[disposal-checker] Started with setInterval fallback.');
  return {
    stop: () => clearInterval(id),
  };
}

module.exports = {
  runDisposalCheck,
  startDisposalChecker,
};
