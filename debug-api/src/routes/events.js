// =============================================================================
// EVENT ROUTES — Query Events from the Event Store
// =============================================================================
// These routes provide rich querying capabilities over the event store.
// Used by engineers (via Debug UI or curl) to investigate distributed workflows.
//
// QUERY PATTERNS:
//   1. By correlation_id → "Show me everything that happened for this request"
//   2. By service        → "Show me what order-service did recently"
//   3. By event type     → "Show me all PAYMENT_FAILED events"
//   4. By time range     → "Show me events from the last hour"
//   5. Recent events     → "Show me the latest 50 events"
//   6. Failures          → "Show me recent failures across all services"
//   7. Search            → "Search for orderId 'ord-abc' in payloads"
//
// ALL queries support pagination via ?limit and ?offset parameters.
// =============================================================================

const express = require('express');

function createEventRoutes(pool) {
    const router = express.Router();

    // =========================================================================
    // GET /events/correlation/:id — Full Event Timeline for a Request
    // =========================================================================
    // THE MOST IMPORTANT ENDPOINT.
    // Returns all events for a correlation_id, ordered by timestamp.
    // This is how engineers reconstruct the full distributed workflow.
    //
    // EXAMPLE:
    //   GET /events/correlation/req-123
    //   → Returns: ORDER_CREATED → PAYMENT_INITIATED → PAYMENT_SUCCESS
    //
    // RESPONSE:
    //   {
    //     events: [...],
    //     summary: {
    //       correlationId, eventCount, services, duration, hasFailures
    //     }
    //   }
    // =========================================================================
    router.get('/correlation/:id', async (req, res) => {
        try {
            const correlationId = req.params.id;

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE correlation_id = $1
                ORDER BY timestamp ASC
            `, [correlationId]);

            const events = result.rows;

            // Build summary
            const summary = {
                correlationId,
                eventCount: events.length,
                services: [...new Set(events.map(e => e.service))],
                eventTypes: events.map(e => e.event_type),
                firstEvent: events.length > 0 ? events[0].timestamp : null,
                lastEvent: events.length > 0 ? events[events.length - 1].timestamp : null,
                duration_ms: events.length > 1
                    ? events[events.length - 1].timestamp - events[0].timestamp
                    : 0,
                hasFailures: events.some(e =>
                    e.event_type.includes('FAILED') || e.event_type.includes('ERROR')
                ),
            };

            res.json({
                success: true,
                events,
                summary,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/service/:name — Events from a Specific Service
    // =========================================================================
    router.get('/service/:name', async (req, res) => {
        try {
            const { limit = 50, offset = 0 } = req.query;

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE service = $1
                ORDER BY timestamp DESC
                LIMIT $2 OFFSET $3
            `, [req.params.name, parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                service: req.params.name,
                events: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/type/:type — Events by Event Type
    // =========================================================================
    router.get('/type/:type', async (req, res) => {
        try {
            const { limit = 50, offset = 0 } = req.query;

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE event_type = $1
                ORDER BY timestamp DESC
                LIMIT $2 OFFSET $3
            `, [req.params.type, parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                eventType: req.params.type,
                events: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/recent — Most Recent Events
    // =========================================================================
    router.get('/recent', async (req, res) => {
        try {
            const { limit = 50, offset = 0 } = req.query;

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                ORDER BY timestamp DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                events: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/failures — Recent Failure Events
    // =========================================================================
    // Quickly shows all failures across all services.
    // Essential for the Debug UI's failure highlighting feature.
    // =========================================================================
    router.get('/failures', async (req, res) => {
        try {
            const { limit = 50, offset = 0 } = req.query;

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE event_type LIKE '%FAILED%' OR event_type LIKE '%ERROR%'
                ORDER BY timestamp DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                events: result.rows,
                count: result.rows.length,
                message: 'Events containing FAILED or ERROR in event_type',
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/search — Full-Text Search in Event Payloads
    // =========================================================================
    // Search for specific values within event payloads.
    // Uses PostgreSQL's ILIKE for case-insensitive pattern matching.
    //
    // EXAMPLE:
    //   GET /events/search?q=ord-abc123
    //   → Returns all events containing "ord-abc123" in their payload
    // =========================================================================
    router.get('/search', async (req, res) => {
        try {
            const { q, limit = 50, offset = 0 } = req.query;

            if (!q) {
                return res.status(400).json({
                    success: false,
                    error: 'Query parameter "q" is required',
                });
            }

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE payload::text ILIKE $1
                   OR correlation_id ILIKE $1
                   OR event_id ILIKE $1
                ORDER BY timestamp DESC
                LIMIT $2 OFFSET $3
            `, [`%${q}%`, parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                query: q,
                events: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /events/range — Events within a time range
    // =========================================================================
    router.get('/range', async (req, res) => {
        try {
            const { start, end, limit = 100, offset = 0 } = req.query;

            if (!start || !end) {
                return res.status(400).json({
                    success: false,
                    error: 'Both "start" and "end" timestamp parameters are required',
                });
            }

            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE timestamp >= $1 AND timestamp <= $2
                ORDER BY timestamp DESC
                LIMIT $3 OFFSET $4
            `, [parseInt(start), parseInt(end), parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                range: { start: parseInt(start), end: parseInt(end) },
                events: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    return router;
}

module.exports = { createEventRoutes };
