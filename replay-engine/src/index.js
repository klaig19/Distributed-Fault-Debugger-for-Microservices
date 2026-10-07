// =============================================================================
// REPLAY ENGINE — Entry Point
// =============================================================================
// The Replay Engine is the CORE INNOVATION of this project.
// It fetches ordered events from the Event Store and re-executes them
// deterministically, allowing engineers to reconstruct system state
// and understand exactly what happened during a distributed transaction.
//
// REPLAY MODES:
//   1. DRY RUN (default, safe)
//      - Simulates event processing without any writes
//      - No database modifications, no side effects
//      - Returns what WOULD happen if events were replayed
//      - Use for: Understanding flow, verifying logic, debugging
//
//   2. STATE REBUILD
//      - Actually re-processes events to rebuild database state
//      - Recreates the exact state that existed after each event
//      - Side effects are still disabled
//      - Use for: Recovering from data corruption, state verification
//
// KEY FEATURES:
//   - Idempotency checking (Redis + PostgreSQL)
//   - Side-effect isolation (no payments, emails, external calls)
//   - Event ordering guarantee (processed in timestamp order)
//   - Progress tracking (WebSocket updates for UI)
//   - Audit logging (every replay session is recorded)
//
// PORT: 3004
// =============================================================================

const express = require('express');
const cors = require('cors');
const { createLogger } = require('@chronoscope/core');
const { createReplayer } = require('./replayer');
const { createIdempotencyChecker } = require('./idempotency');

// =============================================================================
// CONFIGURATION
// =============================================================================
const PORT = process.env.REPLAY_ENGINE_PORT || 3004;
const SERVICE_NAME = 'replay-engine';

// =============================================================================
// INITIALIZATION
// =============================================================================
const logger = createLogger(SERVICE_NAME);
const app = express();

app.use(cors({ origin: '*' }));
app.use(express.json());

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get('/health', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
    });
});

// =============================================================================
// SERVICE STARTUP
// =============================================================================
async function start() {
    try {
        logger.info('Starting Replay Engine...');

        // -----------------------------------------------------------------
        // Step 1: Initialize idempotency checker (Redis)
        // -----------------------------------------------------------------
        const idempotencyChecker = createIdempotencyChecker();
        await idempotencyChecker.connect();
        logger.info('Redis idempotency cache connected');

        // -----------------------------------------------------------------
        // Step 2: Initialize replayer (core replay logic)
        // -----------------------------------------------------------------
        const replayer = createReplayer(idempotencyChecker);
        await replayer.connect();
        logger.info('Replayer initialized');

        // =================================================================
        // API ENDPOINTS
        // =================================================================

        // -----------------------------------------------------------------
        // POST /replay — Execute a replay session
        // -----------------------------------------------------------------
        // Triggers a replay for a specific correlation_id.
        //
        // REQUEST BODY:
        //   {
        //     "correlationId": "req-123",
        //     "mode": "dry-run" | "state-rebuild",
        //     "triggeredBy": "engineer-name"
        //   }
        //
        // RESPONSE:
        //   {
        //     "session": { sessionId, status, results, ... },
        //     "timeline": [ { event, result, duration }, ... ]
        //   }
        // -----------------------------------------------------------------
        app.post('/replay', async (req, res) => {
            try {
                const {
                    correlationId,
                    mode = 'dry-run',
                    triggeredBy = 'api',
                } = req.body;

                if (!correlationId) {
                    return res.status(400).json({
                        success: false,
                        error: 'correlationId is required',
                    });
                }

                if (!['dry-run', 'state-rebuild'].includes(mode)) {
                    return res.status(400).json({
                        success: false,
                        error: 'mode must be "dry-run" or "state-rebuild"',
                    });
                }

                logger.info('Replay requested', {
                    correlationId,
                    mode,
                    triggeredBy,
                });

                const result = await replayer.replay(correlationId, {
                    mode,
                    triggeredBy,
                });

                res.json({
                    success: true,
                    ...result,
                });

            } catch (error) {
                logger.error('Replay failed', { error: error.message });
                res.status(500).json({
                    success: false,
                    error: error.message,
                });
            }
        });

        // -----------------------------------------------------------------
        // GET /replay/sessions — List all replay sessions
        // -----------------------------------------------------------------
        app.get('/replay/sessions', async (req, res) => {
            try {
                const { limit = 20, offset = 0 } = req.query;
                const sessions = await replayer.getSessions(
                    parseInt(limit),
                    parseInt(offset)
                );

                res.json({
                    success: true,
                    sessions,
                });

            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // -----------------------------------------------------------------
        // GET /replay/sessions/:id — Get a specific replay session
        // -----------------------------------------------------------------
        app.get('/replay/sessions/:id', async (req, res) => {
            try {
                const session = await replayer.getSession(req.params.id);

                if (!session) {
                    return res.status(404).json({
                        success: false,
                        error: 'Replay session not found',
                    });
                }

                res.json({
                    success: true,
                    session,
                });

            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // -----------------------------------------------------------------
        // Start Express server
        // -----------------------------------------------------------------
        app.listen(PORT, () => {
            logger.info(`Replay Engine running on port ${PORT}`, {
                port: PORT,
                endpoints: [
                    `GET  http://localhost:${PORT}/health`,
                    `POST http://localhost:${PORT}/replay`,
                    `GET  http://localhost:${PORT}/replay/sessions`,
                    `GET  http://localhost:${PORT}/replay/sessions/:id`,
                ],
            });
        });

        // Graceful shutdown
        const shutdown = async (signal) => {
            logger.info(`${signal} received — shutting down...`);
            await replayer.disconnect();
            await idempotencyChecker.disconnect();
            logger.info('Replay Engine stopped');
            process.exit(0);
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));

    } catch (error) {
        logger.error('Failed to start Replay Engine', {
            error: error.message,
            stack: error.stack,
        });
        process.exit(1);
    }
}

start();
