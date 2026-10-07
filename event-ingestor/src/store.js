// =============================================================================
// EVENT STORE — PostgreSQL Data Access Layer
// =============================================================================
// This module handles all database operations for the event store.
// It provides:
//   1. Connection pooling (for efficient concurrent access)
//   2. Event insertion (with idempotent upsert)
//   3. Event querying (by correlation_id, service, type, time range)
//   4. Timeline reconstruction (ordered events for a request flow)
//   5. Statistics and aggregations
//
// CONNECTION POOLING:
//   PostgreSQL connections are expensive to create. We use a pool of
//   persistent connections that are reused across requests.
//   The pool automatically handles:
//     - Connection creation and destruction
//     - Health checking (ping before use)
//     - Queue management (when all connections are busy)
//
// QUERY OPTIMIZATION:
//   All queries are designed to use the indexes created in init-db.sql:
//     - idx_events_correlation_id: For timeline reconstruction
//     - idx_events_event_type: For type-based filtering
//     - idx_events_service: For service-based filtering
//     - idx_events_timestamp: For time-range queries
//     - idx_events_payload: For JSONB content queries
//
// PREPARED STATEMENTS:
//   Frequently-used queries are defined as constants to avoid SQL injection
//   and enable PostgreSQL query plan caching (faster execution on repeat).
// =============================================================================

const { Pool } = require('pg');
const { createLogger } = require('@chronoscope/core');

// =============================================================================
// DATABASE CONFIGURATION
// =============================================================================
const DB_CONFIG = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'chronoscope',
    user: process.env.DB_USER || 'replay_user',
    password: process.env.DB_PASSWORD || 'replay_pass',
    // Connection pool settings
    max: parseInt(process.env.DB_POOL_MAX || '20'),     // Max connections in pool
    idleTimeoutMillis: 30000,                            // Close idle connections after 30s
    connectionTimeoutMillis: 5000,                       // Timeout waiting for connection
};

// =============================================================================
// SQL QUERIES — Defined as constants for safety and caching
// =============================================================================
// Using parameterized queries ($1, $2, etc.) prevents SQL injection.
// PostgreSQL caches execution plans for repeated queries, improving performance.
// =============================================================================

const QUERIES = {
    // =========================================================================
    // INSERT EVENT (Idempotent Upsert)
    // =========================================================================
    // Uses INSERT ... ON CONFLICT DO NOTHING to handle duplicates.
    //
    // WHY ON CONFLICT DO NOTHING (not DO UPDATE)?
    //   - Events are immutable — once created, they never change
    //   - If we see the same event_id twice, it's a duplicate
    //   - Silently ignoring duplicates is the correct behavior
    //   - This makes the ingestor IDEMPOTENT (safe to re-read from Kafka)
    // =========================================================================
    INSERT_EVENT: `
        INSERT INTO events (event_id, correlation_id, event_type, service, timestamp, payload, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
    `,

    // =========================================================================
    // GET EVENTS BY CORRELATION ID (Timeline Reconstruction)
    // =========================================================================
    // This is the MOST IMPORTANT QUERY in the system.
    // It reconstructs the complete timeline for a distributed request.
    //
    // ORDER BY timestamp ASC: Events shown in chronological order
    // This query uses idx_events_correlation_id for O(log n) lookup
    // =========================================================================
    GET_BY_CORRELATION_ID: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE correlation_id = $1
        ORDER BY timestamp ASC
    `,

    // =========================================================================
    // GET EVENTS BY SERVICE (Service-Level Debugging)
    // =========================================================================
    GET_BY_SERVICE: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE service = $1
        ORDER BY timestamp DESC
        LIMIT $2 OFFSET $3
    `,

    // =========================================================================
    // GET EVENTS BY TYPE (Pattern Detection)
    // =========================================================================
    GET_BY_TYPE: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE event_type = $1
        ORDER BY timestamp DESC
        LIMIT $2 OFFSET $3
    `,

    // =========================================================================
    // GET EVENTS BY TIME RANGE (Recent Activity)
    // =========================================================================
    GET_BY_TIME_RANGE: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE timestamp >= $1 AND timestamp <= $2
        ORDER BY timestamp DESC
        LIMIT $3 OFFSET $4
    `,

    // =========================================================================
    // GET RECENT EVENTS (Dashboard Overview)
    // =========================================================================
    GET_RECENT: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        ORDER BY timestamp DESC
        LIMIT $1 OFFSET $2
    `,

    // =========================================================================
    // GET FAILURE EVENTS (Error Analysis)
    // =========================================================================
    GET_FAILURES: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE event_type LIKE '%FAILED%' OR event_type LIKE '%ERROR%'
        ORDER BY timestamp DESC
        LIMIT $1 OFFSET $2
    `,

    // =========================================================================
    // COUNT EVENTS (Statistics)
    // =========================================================================
    COUNT_ALL: `SELECT COUNT(*) as total FROM events`,
    COUNT_BY_SERVICE: `SELECT service, COUNT(*) as count FROM events GROUP BY service`,
    COUNT_BY_TYPE: `SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY count DESC`,

    // =========================================================================
    // GET UNIQUE CORRELATION IDS (For browsing request flows)
    // =========================================================================
    GET_CORRELATION_IDS: `
        SELECT 
            correlation_id,
            COUNT(*) as event_count,
            MIN(timestamp) as first_event,
            MAX(timestamp) as last_event,
            array_agg(DISTINCT service) as services,
            array_agg(DISTINCT event_type) as event_types
        FROM events
        GROUP BY correlation_id
        ORDER BY MAX(timestamp) DESC
        LIMIT $1 OFFSET $2
    `,

    // =========================================================================
    // SEARCH EVENTS (Full-text search in payload)
    // =========================================================================
    SEARCH_PAYLOAD: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE payload::text ILIKE $1
        ORDER BY timestamp DESC
        LIMIT $2 OFFSET $3
    `,

    // =========================================================================
    // REPLAY SESSION QUERIES
    // =========================================================================
    INSERT_REPLAY_SESSION: `
        INSERT INTO replay_sessions (session_id, correlation_id, mode, status, total_events, triggered_by)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
    `,

    UPDATE_REPLAY_SESSION: `
        UPDATE replay_sessions
        SET status = $2, processed_count = $3, skipped_count = $4, failed_count = $5,
            results = $6, completed_at = $7, error = $8
        WHERE session_id = $1
        RETURNING *
    `,

    GET_REPLAY_SESSION: `
        SELECT * FROM replay_sessions WHERE session_id = $1
    `,

    GET_REPLAY_SESSIONS: `
        SELECT * FROM replay_sessions ORDER BY started_at DESC LIMIT $1 OFFSET $2
    `,

    // =========================================================================
    // IDEMPOTENCY QUERIES
    // =========================================================================
    CHECK_PROCESSED: `
        SELECT event_id FROM processed_events WHERE event_id = $1
    `,

    MARK_PROCESSED: `
        INSERT INTO processed_events (event_id, processed_by, result, error_message)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    `,
};

// =============================================================================
// createEventStore() — Factory function for PostgreSQL event store
// =============================================================================
function createEventStore() {
    const logger = createLogger('event-store');
    let pool = null;

    return {
        // =====================================================================
        // connect() — Initialize connection pool
        // =====================================================================
        async connect() {
            pool = new Pool(DB_CONFIG);

            // Test the connection
            const client = await pool.connect();
            await client.query('SELECT NOW()');
            client.release();

            logger.info('Event Store connected to PostgreSQL', {
                host: DB_CONFIG.host,
                database: DB_CONFIG.database,
                maxConnections: DB_CONFIG.max,
            });

            // Handle pool errors
            pool.on('error', (err) => {
                logger.error('PostgreSQL pool error', { error: err.message });
            });
        },

        // =====================================================================
        // disconnect() — Close all connections
        // =====================================================================
        async disconnect() {
            if (pool) {
                await pool.end();
                logger.info('Event Store disconnected');
            }
        },

        // =====================================================================
        // insertEvent() — Store a single event (idempotent)
        // =====================================================================
        // Uses upsert (ON CONFLICT DO NOTHING) for idempotent writes.
        // Safe to call multiple times with the same event.
        //
        // PARAMETERS:
        //   @param {object} event — The event to store
        //
        // RETURNS:
        //   The inserted event, or null if it was a duplicate
        // =====================================================================
        async insertEvent(event) {
            const result = await pool.query(QUERIES.INSERT_EVENT, [
                event.event_id,
                event.correlation_id,
                event.event_type,
                event.service,
                event.timestamp,
                JSON.stringify(event.payload || {}),
                JSON.stringify(event.metadata || {}),
            ]);

            return result.rows[0] || null;
        },

        // =====================================================================
        // getEventsByCorrelationId() — Timeline Reconstruction
        // =====================================================================
        // THE CORE QUERY: Returns all events for a correlation_id, ordered
        // by timestamp. This is how we reconstruct the full request flow.
        //
        // USAGE:
        //   const timeline = await store.getEventsByCorrelationId('req-123');
        //   // Returns: [ORDER_CREATED, PAYMENT_INITIATED, PAYMENT_SUCCESS]
        //   //          all ordered by timestamp ascending
        // =====================================================================
        async getEventsByCorrelationId(correlationId) {
            const result = await pool.query(QUERIES.GET_BY_CORRELATION_ID, [correlationId]);
            return result.rows;
        },

        // =====================================================================
        // getEventsByService() — Service-level event history
        // =====================================================================
        async getEventsByService(service, limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.GET_BY_SERVICE, [service, limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getEventsByType() — Filter by event type
        // =====================================================================
        async getEventsByType(eventType, limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.GET_BY_TYPE, [eventType, limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getEventsByTimeRange() — Time-range query
        // =====================================================================
        async getEventsByTimeRange(startTime, endTime, limit = 100, offset = 0) {
            const result = await pool.query(QUERIES.GET_BY_TIME_RANGE, [startTime, endTime, limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getRecentEvents() — Most recent events
        // =====================================================================
        async getRecentEvents(limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.GET_RECENT, [limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getFailureEvents() — Events indicating failures
        // =====================================================================
        async getFailureEvents(limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.GET_FAILURES, [limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getCorrelationIds() — List all request flows
        // =====================================================================
        async getCorrelationIds(limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.GET_CORRELATION_IDS, [limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // searchEvents() — Full-text search in payload
        // =====================================================================
        async searchEvents(query, limit = 50, offset = 0) {
            const result = await pool.query(QUERIES.SEARCH_PAYLOAD, [`%${query}%`, limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getStatistics() — Event store statistics
        // =====================================================================
        async getStatistics() {
            const [totalResult, serviceResult, typeResult] = await Promise.all([
                pool.query(QUERIES.COUNT_ALL),
                pool.query(QUERIES.COUNT_BY_SERVICE),
                pool.query(QUERIES.COUNT_BY_TYPE),
            ]);

            return {
                totalEvents: parseInt(totalResult.rows[0].total),
                byService: serviceResult.rows,
                byType: typeResult.rows,
            };
        },

        // =====================================================================
        // REPLAY SESSION METHODS
        // =====================================================================
        async createReplaySession(session) {
            const result = await pool.query(QUERIES.INSERT_REPLAY_SESSION, [
                session.sessionId,
                session.correlationId,
                session.mode,
                session.status || 'PENDING',
                session.totalEvents || 0,
                session.triggeredBy || 'system',
            ]);
            return result.rows[0];
        },

        async updateReplaySession(sessionId, updates) {
            const result = await pool.query(QUERIES.UPDATE_REPLAY_SESSION, [
                sessionId,
                updates.status,
                updates.processedCount || 0,
                updates.skippedCount || 0,
                updates.failedCount || 0,
                JSON.stringify(updates.results || []),
                updates.completedAt || null,
                updates.error || null,
            ]);
            return result.rows[0];
        },

        async getReplaySession(sessionId) {
            const result = await pool.query(QUERIES.GET_REPLAY_SESSION, [sessionId]);
            return result.rows[0] || null;
        },

        async getReplaySessions(limit = 20, offset = 0) {
            const result = await pool.query(QUERIES.GET_REPLAY_SESSIONS, [limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // IDEMPOTENCY METHODS
        // =====================================================================
        async isEventProcessed(eventId) {
            const result = await pool.query(QUERIES.CHECK_PROCESSED, [eventId]);
            return result.rows.length > 0;
        },

        async markEventProcessed(eventId, processedBy, result = 'SUCCESS', errorMessage = null) {
            await pool.query(QUERIES.MARK_PROCESSED, [eventId, processedBy, result, errorMessage]);
        },

        // Expose pool for advanced queries
        getPool() {
            return pool;
        },
    };
}

module.exports = { createEventStore };
