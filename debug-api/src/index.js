// =============================================================================
// DEBUG API — Entry Point
// =============================================================================
// The Debug API is the ENGINEER'S INTERFACE for debugging distributed workflows.
// It aggregates data from the Event Store (PostgreSQL) and Replay Engine,
// providing a unified REST API for:
//
//   1. EVENT QUERYING
//      - GET /events/correlation/:id  → Full event timeline for a request
//      - GET /events/service/:name    → Events from a specific service
//      - GET /events/type/:type       → Events of a specific type
//      - GET /events/recent           → Most recent events
//      - GET /events/failures         → Recent failures
//      - GET /events/search           → Full-text search in payloads
//
//   2. TIMELINE RECONSTRUCTION
//      - GET /timeline/:correlationId → Formatted timeline for UI
//
//   3. REPLAY TRIGGERING
//      - POST /replay                 → Trigger a replay
//      - GET /replay/sessions         → List replay sessions
//
//   4. STATISTICS
//      - GET /stats                   → Event store statistics
//      - GET /flows                   → List all request flows
//
// This API serves as the backend for the Debug UI (React frontend).
//
// PORT: 3005
// =============================================================================

const express = require('express');
const cors = require('cors');
const { createLogger, correlationMiddleware } = require('@chronoscope/core');
const { createEventRoutes } = require('./routes/events');
const { createReplayRoutes } = require('./routes/replay');
const { createTimelineRoutes } = require('./routes/timeline');

// =============================================================================
// CONFIGURATION
// =============================================================================
const PORT = process.env.DEBUG_API_PORT || 3005;
const SERVICE_NAME = 'debug-api';
const REPLAY_ENGINE_URL = process.env.REPLAY_ENGINE_URL || 'http://localhost:3004';

// =============================================================================
// INITIALIZATION
// =============================================================================
const logger = createLogger(SERVICE_NAME);
const app = express();

// Middleware
app.use(cors({
    origin: '*',
    exposedHeaders: ['x-correlation-id'],
}));
app.use(express.json());
app.use(correlationMiddleware(SERVICE_NAME));

// Serve static frontend files
const path = require('path');
const frontendPath = path.join(__dirname, '../../frontend/public');
app.use(express.static(frontendPath));

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get('/health', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '1.0.0',
    });
});

// =============================================================================
// SERVICE STARTUP
// =============================================================================
async function start() {
    try {
        logger.info('Starting Debug API...');

        // -----------------------------------------------------------------
        // Step 1: Initialize PostgreSQL connection
        // -----------------------------------------------------------------
        const { Pool } = require('pg');
        const pool = new Pool({
            host: process.env.DB_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || '5432'),
            database: process.env.DB_NAME || 'chronoscope',
            user: process.env.DB_USER || 'replay_user',
            password: process.env.DB_PASSWORD || 'replay_pass',
            max: 10,
        });

        // Test connection
        const client = await pool.connect();
        await client.query('SELECT NOW()');
        client.release();
        logger.info('PostgreSQL connected');

        // -----------------------------------------------------------------
        // Step 2: Register routes
        // -----------------------------------------------------------------
        const eventRoutes = createEventRoutes(pool);
        const replayRoutes = createReplayRoutes(pool, REPLAY_ENGINE_URL);
        const timelineRoutes = createTimelineRoutes(pool);

        app.use('/events', eventRoutes);
        app.use('/replay', replayRoutes);
        app.use('/timeline', timelineRoutes);

        // -----------------------------------------------------------------
        // Statistics endpoint
        // -----------------------------------------------------------------
        app.get('/stats', async (req, res) => {
            try {
                const [totalResult, serviceResult, typeResult] = await Promise.all([
                    pool.query('SELECT COUNT(*) as total FROM events'),
                    pool.query('SELECT service, COUNT(*) as count FROM events GROUP BY service ORDER BY count DESC'),
                    pool.query('SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY count DESC'),
                ]);

                res.json({
                    success: true,
                    stats: {
                        totalEvents: parseInt(totalResult.rows[0].total),
                        byService: serviceResult.rows,
                        byType: typeResult.rows,
                    },
                });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // -----------------------------------------------------------------
        // Flows endpoint — list all correlation IDs (request flows)
        // -----------------------------------------------------------------
        app.get('/flows', async (req, res) => {
            try {
                const { limit = 50, offset = 0 } = req.query;
                const result = await pool.query(`
                    SELECT 
                        correlation_id,
                        COUNT(*) as event_count,
                        MIN(timestamp) as first_event,
                        MAX(timestamp) as last_event,
                        MAX(timestamp) - MIN(timestamp) as duration_ms,
                        array_agg(DISTINCT service) as services,
                        array_agg(DISTINCT event_type) as event_types,
                        bool_or(event_type LIKE '%FAILED%') as has_failure
                    FROM events
                    GROUP BY correlation_id
                    ORDER BY MAX(timestamp) DESC
                    LIMIT $1 OFFSET $2
                `, [parseInt(limit), parseInt(offset)]);

                res.json({
                    success: true,
                    flows: result.rows,
                    total: result.rows.length,
                });
            } catch (error) {
                res.status(500).json({ success: false, error: error.message });
            }
        });

        // -----------------------------------------------------------------
        // Start server
        // -----------------------------------------------------------------
        app.listen(PORT, () => {
            logger.info(`Debug API running on port ${PORT}`, {
                port: PORT,
                endpoints: [
                    `GET  http://localhost:${PORT}/health`,
                    `GET  http://localhost:${PORT}/stats`,
                    `GET  http://localhost:${PORT}/flows`,
                    `GET  http://localhost:${PORT}/events/correlation/:id`,
                    `GET  http://localhost:${PORT}/events/recent`,
                    `GET  http://localhost:${PORT}/events/failures`,
                    `GET  http://localhost:${PORT}/timeline/:correlationId`,
                    `POST http://localhost:${PORT}/replay`,
                ],
            });
        });

        // Graceful shutdown
        const shutdown = async (signal) => {
            logger.info(`${signal} received — shutting down...`);
            await pool.end();
            logger.info('Debug API stopped');
            process.exit(0);
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));

    } catch (error) {
        logger.error('Failed to start Debug API', {
            error: error.message,
            stack: error.stack,
        });
        process.exit(1);
    }
}

start();
