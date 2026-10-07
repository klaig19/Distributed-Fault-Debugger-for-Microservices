// =============================================================================
// REPLAY ROUTES — Trigger and Monitor Replay Sessions
// =============================================================================
// These routes provide the interface for triggering replays and monitoring
// their progress. They proxy requests to the Replay Engine service.
//
// WHY PROXY THROUGH DEBUG API?
//   - Single entry point for the Debug UI (one API to talk to)
//   - Can add authentication/authorization before forwarding
//   - Can enrich replay results with additional context
//   - Decouples the UI from the internal service topology
// =============================================================================

const express = require('express');

function createReplayRoutes(pool, replayEngineUrl) {
    const router = express.Router();

    // =========================================================================
    // POST /replay — Trigger a Replay Session
    // =========================================================================
    // Initiates a deterministic replay for a given correlation_id.
    //
    // REQUEST BODY:
    //   {
    //     "correlationId": "req-123",
    //     "mode": "dry-run" | "state-rebuild"
    //   }
    //
    // FLOW:
    //   1. Validate inputs
    //   2. Verify events exist for this correlation_id
    //   3. Forward request to Replay Engine
    //   4. Return results
    // =========================================================================
    router.post('/', async (req, res) => {
        try {
            const { correlationId, mode = 'dry-run' } = req.body;

            if (!correlationId) {
                return res.status(400).json({
                    success: false,
                    error: 'correlationId is required',
                });
            }

            // Verify events exist before forwarding to replay engine
            const eventsResult = await pool.query(
                'SELECT COUNT(*) as count FROM events WHERE correlation_id = $1',
                [correlationId]
            );

            const eventCount = parseInt(eventsResult.rows[0].count);
            if (eventCount === 0) {
                return res.status(404).json({
                    success: false,
                    error: `No events found for correlation_id: ${correlationId}`,
                });
            }

            // Forward to Replay Engine
            const response = await fetch(`${replayEngineUrl}/replay`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    correlationId,
                    mode,
                    triggeredBy: req.body.triggeredBy || 'debug-api',
                }),
            });

            const result = await response.json();

            res.json({
                success: true,
                ...result,
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message,
                hint: 'Make sure the Replay Engine is running on ' + replayEngineUrl,
            });
        }
    });

    // =========================================================================
    // GET /replay/sessions — List All Replay Sessions
    // =========================================================================
    router.get('/sessions', async (req, res) => {
        try {
            const { limit = 20, offset = 0 } = req.query;

            const result = await pool.query(`
                SELECT * FROM replay_sessions
                ORDER BY started_at DESC
                LIMIT $1 OFFSET $2
            `, [parseInt(limit), parseInt(offset)]);

            res.json({
                success: true,
                sessions: result.rows,
                count: result.rows.length,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    // =========================================================================
    // GET /replay/sessions/:id — Get a Specific Replay Session
    // =========================================================================
    router.get('/sessions/:id', async (req, res) => {
        try {
            const result = await pool.query(
                'SELECT * FROM replay_sessions WHERE session_id = $1',
                [req.params.id]
            );

            if (result.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: 'Replay session not found',
                });
            }

            res.json({
                success: true,
                session: result.rows[0],
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    return router;
}

module.exports = { createReplayRoutes };
