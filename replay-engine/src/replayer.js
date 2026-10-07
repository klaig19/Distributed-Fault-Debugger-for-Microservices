// =============================================================================
// REPLAYER — Core Deterministic Replay Logic
// =============================================================================
// This is the HEART of the entire project. The replayer:
//   1. Fetches ordered events from the Event Store (PostgreSQL)
//   2. For each event, checks idempotency (Redis + PostgreSQL)
//   3. Routes to the appropriate replay handler based on event type
//   4. Executes the handler in the appropriate mode (dry-run / state-rebuild)
//   5. Records the result of each event replay
//   6. Creates an audit trail (replay session) for the entire replay
//
// DETERMINISTIC GUARANTEE:
//   Events are always processed in timestamp order (ascending).
//   Given the same set of events, the replay will always produce
//   the same result. This is what "deterministic" means.
//
// REPLAY FLOW:
//   ┌─────────────────────┐
//   │ Fetch Events (DB)   │  SELECT * FROM events WHERE correlation_id = ?
//   └─────────┬───────────┘  ORDER BY timestamp ASC
//             │
//             ▼
//   ┌─────────────────────┐
//   │ For Each Event:     │
//   │  1. Check idempotency│ Is this event already processed?
//   │  2. Get handler      │ Route by event_type
//   │  3. Execute handler  │ dry-run or state-rebuild
//   │  4. Record result    │ Success, skipped, or failed
//   └─────────┬───────────┘
//             │
//             ▼
//   ┌─────────────────────┐
//   │ Create Audit Record │  Save replay session to DB
//   └─────────────────────┘
//
// HANDLER REGISTRY:
//   Each event type maps to a handler function that knows how to
//   "replay" that specific event. Handlers have two modes:
//     - dry-run: Log what would happen, return simulated result
//     - state-rebuild: Actually apply the state change
//

// =============================================================================

const { v4: uuidv4 } = require('uuid');
const { Pool } = require('pg');
const { createLogger, EventTypes } = require('@chronoscope/core');
const { createOrderReplayHandler } = require('./handlers/order-replay-handler');
const { createPaymentReplayHandler } = require('./handlers/payment-replay-handler');

// =============================================================================
// DATABASE CONFIGURATION (connects to same PostgreSQL as event-ingestor)
// =============================================================================
const DB_CONFIG = {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || '5432'),
    database: process.env.DB_NAME || 'chronoscope',
    user: process.env.DB_USER || 'replay_user',
    password: process.env.DB_PASSWORD || 'replay_pass',
    max: 10,
};

// =============================================================================
// SQL QUERIES
// =============================================================================
const QUERIES = {
    // Fetch events for replay — ordered by timestamp for deterministic processing
    GET_EVENTS_FOR_REPLAY: `
        SELECT event_id, correlation_id, event_type, service, timestamp, payload, metadata, created_at
        FROM events
        WHERE correlation_id = $1
        ORDER BY timestamp ASC
    `,

    // Replay session management
    INSERT_REPLAY_SESSION: `
        INSERT INTO replay_sessions (session_id, correlation_id, mode, status, total_events, triggered_by)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
    `,

    UPDATE_REPLAY_SESSION: `
        UPDATE replay_sessions
        SET status = $2, processed_count = $3, skipped_count = $4, failed_count = $5,
            results = $6, completed_at = NOW(), error = $7
        WHERE session_id = $1
        RETURNING *
    `,

    GET_REPLAY_SESSION: `SELECT * FROM replay_sessions WHERE session_id = $1`,

    GET_REPLAY_SESSIONS: `SELECT * FROM replay_sessions ORDER BY started_at DESC LIMIT $1 OFFSET $2`,

    // Idempotency check in PostgreSQL (fallback if Redis is unavailable).
    // Only successful processing counts — a FAILED row must not block a retry.
    CHECK_PROCESSED: `SELECT event_id FROM processed_events WHERE event_id = $1 AND result = 'SUCCESS'`,

    MARK_PROCESSED: `
        INSERT INTO processed_events (event_id, processed_by, result, error_message)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING
    `,
};

// =============================================================================
// createReplayer() — Factory function for the replay engine core
// =============================================================================
// PARAMETERS:
//   @param {object} idempotencyChecker — Redis-based idempotency checker
//
// RETURNS:
//   Object with replay(), getSessions(), getSession(), connect(), disconnect()
// =============================================================================
function createReplayer(idempotencyChecker) {
    const logger = createLogger('replayer');
    let pool = null;

    // =========================================================================
    // REPLAY HANDLER REGISTRY
    // =========================================================================
    // Maps event types to their replay handler functions.
    // Each handler knows how to "replay" a specific type of event.
    //
    // WHY A REGISTRY?
    //   - Clean separation of concerns (each handler is independent)
    //   - Easy to add new event types (just add a handler + register it)
    //   - Testable (test each handler in isolation)
    //   - Extensible (swap handlers for different replay behaviors)
    //
    // ADDING A NEW EVENT TYPE:
    //   1. Create a handler in ./handlers/
    //   2. Register it in the handlerRegistry below
    //   3. The replayer will automatically route events to it
    // =========================================================================
    // Handlers receive a pool getter so state-rebuild mode can persist to
    // the orders/payments tables. A getter (not the pool itself) because
    // the pool doesn't exist until connect() runs.
    const getPool = () => pool;
    const orderReplayHandler = createOrderReplayHandler(getPool);
    const paymentReplayHandler = createPaymentReplayHandler(getPool);

    const handlerRegistry = {
        // Order events
        [EventTypes.ORDER_CREATED]:   orderReplayHandler.handleOrderCreated,
        [EventTypes.ORDER_UPDATED]:   orderReplayHandler.handleOrderUpdated,
        [EventTypes.ORDER_CANCELLED]: orderReplayHandler.handleOrderCancelled,
        [EventTypes.ORDER_COMPLETED]: orderReplayHandler.handleOrderCompleted,

        // Payment events
        [EventTypes.PAYMENT_INITIATED]:  paymentReplayHandler.handlePaymentInitiated,
        [EventTypes.PAYMENT_SUCCESS]:    paymentReplayHandler.handlePaymentSuccess,
        [EventTypes.PAYMENT_FAILED]:     paymentReplayHandler.handlePaymentFailed,
        [EventTypes.PAYMENT_REFUNDED]:   paymentReplayHandler.handlePaymentRefunded,

        // System events (logged but not replayed)
        [EventTypes.REPLAY_STARTED]:   async (event, mode) => ({ action: 'logged', details: 'System event — no replay action' }),
        [EventTypes.REPLAY_COMPLETED]: async (event, mode) => ({ action: 'logged', details: 'System event — no replay action' }),
    };

    return {
        // =====================================================================
        // connect() — Initialize database connection pool
        // =====================================================================
        async connect() {
            pool = new Pool(DB_CONFIG);
            const client = await pool.connect();
            await client.query('SELECT NOW()');
            client.release();
            logger.info('Replayer connected to PostgreSQL');
        },

        // =====================================================================
        // disconnect() — Close database connections
        // =====================================================================
        async disconnect() {
            if (pool) await pool.end();
        },

        // =====================================================================
        // replay() — Execute a deterministic replay for a correlation_id
        // =====================================================================
        // This is the MAIN METHOD that engineers interact with.
        //
        // ALGORITHM:
        //   1. Fetch all events for the correlation_id (ordered by timestamp)
        //   2. Create a replay session record (audit trail)
        //   3. For each event (in order):
        //      a. Check if event was already processed (idempotency)
        //      b. If already processed → skip (increment skipped counter)
        //      c. If not processed → find handler, execute, record result
        //      d. Mark event as processed (for future idempotency)
        //   4. Update replay session with final results
        //   5. Return complete replay report
        //
        // PARAMETERS:
        //   @param {string} correlationId — The request flow to replay
        //   @param {object} options       — { mode, triggeredBy }
        //
        // RETURNS:
        //   {
        //     session: { sessionId, status, processedCount, ... },
        //     timeline: [
        //       { event, result, duration, skipped },
        //       ...
        //     ]
        //   }
        // =====================================================================
        async replay(correlationId, options = {}) {
            const {
                mode = 'dry-run',
                triggeredBy = 'api',
            } = options;

            const sessionId = `replay-${uuidv4().substring(0, 8)}`;
            const startTime = Date.now();

            logger.info('Starting replay session', {
                sessionId,
                correlationId,
                mode,
                triggeredBy,
            });

            // -----------------------------------------------------------------
            // Step 1: Fetch all events for this correlation_id
            // -----------------------------------------------------------------
            // Events are ordered by timestamp ASC for deterministic processing.
            // This is the EXACT order they occurred in the real system.
            // -----------------------------------------------------------------
            const eventsResult = await pool.query(QUERIES.GET_EVENTS_FOR_REPLAY, [correlationId]);
            const events = eventsResult.rows;

            if (events.length === 0) {
                logger.warn('No events found for correlation_id', { correlationId });
                return {
                    session: {
                        sessionId,
                        correlationId,
                        mode,
                        status: 'COMPLETED',
                        totalEvents: 0,
                        message: 'No events found for this correlation_id',
                    },
                    timeline: [],
                };
            }

            logger.info(`Found ${events.length} events to replay`, {
                sessionId,
                correlationId,
                eventTypes: events.map(e => e.event_type),
            });

            // -----------------------------------------------------------------
            // Step 2: Create replay session record (audit trail)
            // -----------------------------------------------------------------
            await pool.query(QUERIES.INSERT_REPLAY_SESSION, [
                sessionId,
                correlationId,
                mode,
                'RUNNING',
                events.length,
                triggeredBy,
            ]);

            // -----------------------------------------------------------------
            // Step 3: Process each event in order
            // -----------------------------------------------------------------
            // This is the core replay loop. Events are processed SEQUENTIALLY
            // in timestamp order. This guarantees deterministic behavior.
            //
            // For each event:
            //   1. Check idempotency → has this event been replayed before?
            //   2. Find handler → what function handles this event type?
            //   3. Execute handler → process the event
            //   4. Record result → track success/failure/skip
            // -----------------------------------------------------------------
            const timeline = [];
            let processedCount = 0;
            let skippedCount = 0;
            let failedCount = 0;
            let overallStatus = 'COMPLETED';

            for (const event of events) {
                const eventStartTime = Date.now();

                try {
                    // ---------------------------------------------------------
                    // Step 3a: Idempotency Check
                    // ---------------------------------------------------------
                    // Before processing, check if this event was already replayed.
                    // This prevents:
                    //   - Duplicate state changes
                    //   - Incorrect counters
                    //   - Corrupted aggregate state
                    //
                    // Check order: Redis (fast) → PostgreSQL (fallback)
                    //
                    // MODE-AWARE DEDUP:
                    //   dry-run       → never skip. Dry runs are pure
                    //                   simulations; engineers expect to
                    //                   re-run them freely.
                    //   state-rebuild → skip if ANY prior replay already
                    //                   applied this event. Keys are GLOBAL
                    //                   (not session-scoped) so dedup works
                    //                   across replay sessions, and the PG
                    //                   processed_events table is consulted
                    //                   when Redis misses (restart/TTL expiry).
                    // ---------------------------------------------------------
                    let alreadyProcessed = false;
                    if (mode === 'state-rebuild') {
                        alreadyProcessed = await idempotencyChecker.isProcessed(event.event_id);
                        if (!alreadyProcessed) {
                            const pgCheck = await pool.query(QUERIES.CHECK_PROCESSED, [event.event_id]);
                            alreadyProcessed = pgCheck.rows.length > 0;
                        }
                    }

                    if (alreadyProcessed) {
                        // Event already processed — skip it
                        skippedCount++;
                        timeline.push({
                            event_id: event.event_id,
                            event_type: event.event_type,
                            service: event.service,
                            timestamp: event.timestamp,
                            status: 'SKIPPED',
                            reason: 'Already processed (idempotency check)',
                            duration_ms: Date.now() - eventStartTime,
                        });

                        logger.debug('Event skipped (idempotent)', {
                            event_id: event.event_id,
                            event_type: event.event_type,
                        });

                        continue;
                    }

                    // ---------------------------------------------------------
                    // Step 3b: Find and execute handler
                    // ---------------------------------------------------------
                    const handler = handlerRegistry[event.event_type];

                    if (!handler) {
                        // No handler registered for this event type
                        // Log and skip — don't fail the entire replay
                        skippedCount++;
                        timeline.push({
                            event_id: event.event_id,
                            event_type: event.event_type,
                            service: event.service,
                            timestamp: event.timestamp,
                            status: 'SKIPPED',
                            reason: `No replay handler registered for event type: ${event.event_type}`,
                            duration_ms: Date.now() - eventStartTime,
                        });

                        logger.warn('No handler for event type', {
                            event_type: event.event_type,
                        });

                        continue;
                    }

                    // ---------------------------------------------------------
                    // Step 3c: Execute the handler
                    // ---------------------------------------------------------
                    // Pass the event and mode to the handler.
                    // The handler decides what to do based on the mode:
                    //   dry-run → simulate, log, return what would happen
                    //   state-rebuild → actually apply state changes
                    // ---------------------------------------------------------
                    logger.info('Replaying event', {
                        event_id: event.event_id,
                        event_type: event.event_type,
                        service: event.service,
                        mode,
                        sessionId,
                    });

                    // Parse payload if it's a string (from PostgreSQL)
                    const eventWithParsedPayload = {
                        ...event,
                        payload: typeof event.payload === 'string'
                            ? JSON.parse(event.payload)
                            : event.payload,
                    };

                    const result = await handler(eventWithParsedPayload, mode);

                    // ---------------------------------------------------------
                    // Step 3d: Record success and mark as processed
                    // ---------------------------------------------------------
                    processedCount++;

                    // Only state-rebuild leaves an idempotency footprint —
                    // dry-run makes no state changes, so recording it would
                    // wrongly block future rebuilds. Keys are global so the
                    // dedup holds across sessions.
                    if (mode === 'state-rebuild') {
                        // Mark as processed in Redis (for future idempotency checks)
                        await idempotencyChecker.markProcessed(event.event_id);

                        // Mark as processed in PostgreSQL (persistent backup)
                        await pool.query(QUERIES.MARK_PROCESSED, [
                            event.event_id,
                            'replay-engine',
                            'SUCCESS',
                            null,
                        ]);
                    }

                    timeline.push({
                        event_id: event.event_id,
                        event_type: event.event_type,
                        service: event.service,
                        timestamp: event.timestamp,
                        status: 'PROCESSED',
                        mode,
                        result,
                        duration_ms: Date.now() - eventStartTime,
                    });

                    logger.info('Event replayed successfully', {
                        event_id: event.event_id,
                        event_type: event.event_type,
                        duration_ms: Date.now() - eventStartTime,
                    });

                } catch (error) {
                    // ---------------------------------------------------------
                    // Event replay failed — record error but continue
                    // ---------------------------------------------------------
                    failedCount++;

                    // Record the failure in PostgreSQL (audit only — a FAILED
                    // row never blocks a retry; CHECK_PROCESSED filters on SUCCESS)
                    if (mode === 'state-rebuild') {
                        await pool.query(QUERIES.MARK_PROCESSED, [
                            event.event_id,
                            'replay-engine',
                            'FAILED',
                            error.message,
                        ]).catch(() => {}); // Don't fail the loop on recording failure
                    }

                    timeline.push({
                        event_id: event.event_id,
                        event_type: event.event_type,
                        service: event.service,
                        timestamp: event.timestamp,
                        status: 'FAILED',
                        error: error.message,
                        duration_ms: Date.now() - eventStartTime,
                    });

                    logger.error('Event replay failed', {
                        event_id: event.event_id,
                        event_type: event.event_type,
                        error: error.message,
                    });

                    overallStatus = 'COMPLETED_WITH_ERRORS';
                }
            }

            // -----------------------------------------------------------------
            // Step 4: Update replay session with final results
            // -----------------------------------------------------------------
            const totalDuration = Date.now() - startTime;

            await pool.query(QUERIES.UPDATE_REPLAY_SESSION, [
                sessionId,
                overallStatus,
                processedCount,
                skippedCount,
                failedCount,
                JSON.stringify(timeline),
                null, // error
            ]);

            const session = {
                sessionId,
                correlationId,
                mode,
                status: overallStatus,
                totalEvents: events.length,
                processedCount,
                skippedCount,
                failedCount,
                triggeredBy,
                duration_ms: totalDuration,
            };

            logger.info('Replay session completed', {
                ...session,
                eventsPerSecond: (events.length / (totalDuration / 1000)).toFixed(2),
            });

            return { session, timeline };
        },

        // =====================================================================
        // getSessions() — List replay sessions (for UI)
        // =====================================================================
        async getSessions(limit = 20, offset = 0) {
            const result = await pool.query(QUERIES.GET_REPLAY_SESSIONS, [limit, offset]);
            return result.rows;
        },

        // =====================================================================
        // getSession() — Get a single replay session (for UI)
        // =====================================================================
        async getSession(sessionId) {
            const result = await pool.query(QUERIES.GET_REPLAY_SESSION, [sessionId]);
            return result.rows[0] || null;
        },
    };
}

module.exports = { createReplayer };
