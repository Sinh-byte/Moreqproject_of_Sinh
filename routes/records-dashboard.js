'use strict';

const express = require('express');

function safeJsonParse(s, fallback) {
  try {
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}

function createRecordsDashboardRouter(db, requireAuth) {
  const router = express.Router();
  router.use(requireAuth);

  router.get('/stats', (_req, res) => {
    try {
      const row = db.prepare(`
        SELECT
          (SELECT COUNT(1) FROM rm_records) AS totalRecords,
          (
            SELECT json_group_object(state, c)
            FROM (SELECT state, COUNT(1) AS c FROM rm_records GROUP BY state)
          ) AS byState,
          (
            SELECT json_group_object(doc_type, c)
            FROM (SELECT doc_type, COUNT(1) AS c FROM rm_records GROUP BY doc_type)
          ) AS byDocType,
          (
            SELECT json_group_array(json_object('month', month, 'count', c))
            FROM (
              SELECT strftime('%Y-%m', capture_date) AS month, COUNT(1) AS c
              FROM rm_records
              WHERE datetime(capture_date) >= datetime('now', '-12 months')
              GROUP BY strftime('%Y-%m', capture_date)
              ORDER BY month
            )
          ) AS captureByMonth,
          (
            SELECT json_group_array(
              json_object(
                'id', id,
                'record_number', record_number,
                'title', title,
                'retention_due_date', retention_due_date,
                'days_to_due', CAST(julianday(date(retention_due_date)) - julianday(date('now')) AS INTEGER)
              )
            )
            FROM (
              SELECT id, record_number, title, retention_due_date
              FROM rm_records
              WHERE retention_due_date IS NOT NULL
              ORDER BY date(retention_due_date) ASC
              LIMIT 30
            )
          ) AS disposalTimeline,
          (
            SELECT json_object(
              'hasClassification', (SELECT COUNT(1) FROM rm_records WHERE classification_id IS NOT NULL),
              'hasDisposalSchedule', (SELECT COUNT(1) FROM rm_records WHERE disposal_schedule_id IS NOT NULL),
              'hasComponent', (SELECT COUNT(1) FROM rm_components),
              'hasAgents', (SELECT COUNT(1) FROM users),
              'hashIntegrity', (
                SELECT CASE
                  WHEN COUNT(1) = 0 THEN 1
                  WHEN SUM(CASE WHEN prev_ok THEN 1 ELSE 0 END) = COUNT(1) THEN 1
                  ELSE 0
                END
                FROM (
                  SELECT
                    e.id,
                    CASE
                      WHEN LEAD(e.event_hash) OVER (ORDER BY e.occurred_at DESC, e.id DESC) IS NULL THEN 1
                      WHEN e.prev_event_hash = LEAD(e.event_hash) OVER (ORDER BY e.occurred_at DESC, e.id DESC) THEN 1
                      ELSE 0
                    END AS prev_ok
                  FROM rm_event_history e
                  ORDER BY e.occurred_at DESC, e.id DESC
                  LIMIT 100
                )
              )
            )
          ) AS healthMetrics,
          (
            SELECT json_group_array(
              json_object(
                'actor', actor,
                'count', c
              )
            )
            FROM (
              SELECT COALESCE(actor_label, actor_id, 'SYSTEM') AS actor, COUNT(1) AS c
              FROM rm_event_history
              WHERE datetime(occurred_at) >= datetime('now', '-30 days')
              GROUP BY COALESCE(actor_label, actor_id, 'SYSTEM')
              ORDER BY c DESC
              LIMIT 20
            )
          ) AS activityByUser
      `).get();

      return res.json({
        totalRecords: row.totalRecords || 0,
        byState: safeJsonParse(row.byState || '{}', {}),
        byDocType: safeJsonParse(row.byDocType || '{}', {}),
        captureByMonth: safeJsonParse(row.captureByMonth || '[]', []),
        disposalTimeline: safeJsonParse(row.disposalTimeline || '[]', []),
        healthMetrics: safeJsonParse(row.healthMetrics || '{}', {}),
        activityByUser: safeJsonParse(row.activityByUser || '[]', []),
      });
    } catch (e) {
      return res.status(500).json({ error: e.message || 'Lỗi dashboard stats' });
    }
  });

  return router;
}

module.exports = createRecordsDashboardRouter;
