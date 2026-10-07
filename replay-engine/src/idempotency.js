// =============================================================================
// IDEMPOTENCY CHECKER — Redis-Based Duplicate Prevention
// =============================================================================
// This module provides O(1) idempotency checks using Redis.
//
// WHAT IS IDEMPOTENCY?
//   An operation is idempotent if performing it multiple times produces
//   the same result as performing it once. In our context:
//     - Processing an event once → order created, payment charged
//     - Processing the same event twice → should NOT create a second order
//       or charge the customer again
//
// WHY IS THIS CRITICAL FOR REPLAY?
//   During replay, we re-process events that may have already been processed.
//   Without idempotency:
//     - Replay would create duplicate orders
//     - Replay would double-charge customers
//     - Replay would send duplicate notifications
//     - System state would be corrupted
//
// DUAL STORAGE STRATEGY:
//   ┌─────────────┐     ┌──────────────────┐
//   │   Redis      │     │   PostgreSQL      │
//   │ (Hot Path)   │     │   (Cold Path)     │
//   │ O(1) lookup  │     │   Persistent      │
//   │ TTL: 24h     │     │   No expiration   │
//   │ 100k+ ops/s  │     │   Audit trail     │
//   └─────────────┘     └──────────────────┘
//
//   1. CHECK: First check Redis (fast) → then PostgreSQL (fallback)
//   2. MARK:  Write to both Redis AND PostgreSQL
//
//   Redis provides speed (sub-millisecond checks).
//   PostgreSQL provides durability (survives Redis restarts).
//
// REDIS KEY PATTERN:
//   Key:   "idempotent:{event_id}"
//   Value: JSON { processedAt, sessionId }
//   TTL:   24 hours (configurable)
//

// =============================================================================

const Redis = require('ioredis');
const { createLogger } = require('@chronoscope/core');

// =============================================================================
// CONFIGURATION
// =============================================================================
const REDIS_CONFIG = {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379'),
    // Retry strategy: exponential backoff with jitter
    retryStrategy: (times) => {
        if (times > 10) return null; // Give up after 10 retries
        return Math.min(times * 200, 5000); // Max 5 second delay
    },
    lazyConnect: true, // Don't connect until explicitly called
};

// Key prefix for idempotency entries in Redis
const KEY_PREFIX = 'idempotent:';

// Default TTL for idempotency entries (24 hours in seconds)
const DEFAULT_TTL = 24 * 60 * 60;

// =============================================================================
// createIdempotencyChecker() — Factory function for Redis idempotency layer
// =============================================================================
// RETURNS:
//   Object with connect(), disconnect(), isProcessed(), markProcessed(),
//   clear(), getStats()
// =============================================================================
function createIdempotencyChecker(options = {}) {
    const logger = createLogger('idempotency');
    const ttl = options.ttl || DEFAULT_TTL;
    let redis = null;
    let isConnected = false;

    // Stats tracking
    const stats = {
        checks: 0,           // Total isProcessed() calls
        hits: 0,             // Times we found a processed event (duplicate detected)
        misses: 0,           // Times the event was not yet processed
        marked: 0,           // Times we marked an event as processed
        errors: 0,           // Redis errors (fallback to PostgreSQL)
    };

    return {
        // =====================================================================
        // connect() — Initialize Redis connection
        // =====================================================================
        async connect() {
            redis = new Redis(REDIS_CONFIG);

            try {
                await redis.connect();
                isConnected = true;

                // Verify connection
                await redis.ping();

                logger.info('Redis idempotency cache connected', {
                    host: REDIS_CONFIG.host,
                    port: REDIS_CONFIG.port,
                    ttl: `${ttl}s (${ttl / 3600}h)`,
                });
            } catch (error) {
                logger.warn('Redis connection failed — will use PostgreSQL fallback', {
                    error: error.message,
                });
                isConnected = false;
            }

            // Handle connection errors gracefully
            if (redis) {
                redis.on('error', (err) => {
                    logger.warn('Redis error', { error: err.message });
                    stats.errors++;
                });

                redis.on('reconnecting', () => {
                    logger.info('Redis reconnecting...');
                });
            }
        },

        // =====================================================================
        // disconnect() — Close Redis connection
        // =====================================================================
        async disconnect() {
            if (redis) {
                await redis.quit();
                isConnected = false;
                logger.info('Redis disconnected', { stats });
            }
        },

        // =====================================================================
        // isProcessed() — Check if an event has already been processed
        // =====================================================================
        // This is the HOT PATH — called for every event during replay.
        // Must be as fast as possible (sub-millisecond target).
        //
        // PARAMETERS:
        //   @param {string} eventId   — The event ID to check
        //   @param {string} sessionId — The current replay session ID
        //
        // RETURNS:
        //   boolean — true if event was already processed, false otherwise
        //
        // ALGORITHM:
        //   1. Check Redis (if connected) → O(1) lookup
        //   2. If Redis is unavailable → return false (let PostgreSQL handle it)
        //   3. Log the result for debugging
        //
        // PERFORMANCE:
        //   Redis GET: ~0.1ms
        //   PostgreSQL SELECT: ~1-5ms
        //   This 10-50x speedup matters when replaying thousands of events
        // =====================================================================
        async isProcessed(eventId, sessionId = null) {
            stats.checks++;

            if (!isConnected || !redis) {
                // Redis unavailable — assume not processed
                // PostgreSQL will be checked as fallback by the replayer
                stats.misses++;
                return false;
            }

            try {
                // Determine the key based on whether a sessionId is provided.
                // This scopes the idempotency to a specific replay session!
                const key = sessionId 
                    ? `${KEY_PREFIX}${sessionId}:${eventId}` 
                    : `${KEY_PREFIX}${eventId}`;
                    
                const result = await redis.get(key);

                if (result) {
                    stats.hits++;
                    logger.debug('Idempotency HIT — event already processed', {
                        eventId,
                        processedData: JSON.parse(result),
                    });
                    return true;
                }

                stats.misses++;
                return false;

            } catch (error) {
                stats.errors++;
                logger.warn('Redis check failed — assuming not processed', {
                    eventId,
                    error: error.message,
                });
                return false;
            }
        },

        // =====================================================================
        // markProcessed() — Mark an event as processed
        // =====================================================================
        // Called AFTER successfully processing an event during replay.
        // Writes to Redis with TTL for automatic cleanup.
        //
        // PARAMETERS:
        //   @param {string} eventId   — The event ID to mark
        //   @param {string} sessionId — The replay session that processed it
        //
        // TTL STRATEGY:
        //   - 24 hours by default
        //   - Long enough for debugging sessions (usually < 1 hour)
        //   - Short enough to prevent Redis memory bloat
        //   - Can be configured via options.ttl
        // =====================================================================
        async markProcessed(eventId, sessionId = null) {
            stats.marked++;

            if (!isConnected || !redis) {
                return; // PostgreSQL will handle persistence
            }

            try {
                // Scope the key by sessionId if it exists
                const key = sessionId 
                    ? `${KEY_PREFIX}${sessionId}:${eventId}` 
                    : `${KEY_PREFIX}${eventId}`;
                    
                const value = JSON.stringify({
                    processedAt: new Date().toISOString(),
                    sessionId,
                    service: 'replay-engine',
                });

                // SET with TTL — automatically expires after 24 hours
                await redis.setex(key, ttl, value);

                logger.debug('Event marked as processed in Redis', {
                    eventId,
                    ttl: `${ttl}s`,
                });

            } catch (error) {
                stats.errors++;
                logger.warn('Failed to mark event in Redis', {
                    eventId,
                    error: error.message,
                });
                // Not critical — PostgreSQL backup will handle this
            }
        },

        // =====================================================================
        // clear() — Clear all idempotency entries (for testing/reset)
        // =====================================================================
        // USE WITH CAUTION: This removes all idempotency protection.
        // Only use for:
        //   - Testing (start with clean state)
        //   - Intentional re-replay (after fixing a bug in a handler)
        // =====================================================================
        async clear(pattern = '*') {
            if (!isConnected || !redis) return;

            try {
                const keys = await redis.keys(`${KEY_PREFIX}${pattern}`);
                if (keys.length > 0) {
                    await redis.del(...keys);
                    logger.info(`Cleared ${keys.length} idempotency entries`);
                }
            } catch (error) {
                logger.error('Failed to clear idempotency cache', {
                    error: error.message,
                });
            }
        },

        // =====================================================================
        // getStats() — Return idempotency cache statistics
        // =====================================================================
        getStats() {
            return {
                ...stats,
                hitRate: stats.checks > 0
                    ? `${((stats.hits / stats.checks) * 100).toFixed(1)}%`
                    : '0%',
                connected: isConnected,
            };
        },
    };
}

module.exports = { createIdempotencyChecker };
