// =============================================================================
// EVENT INGESTOR — Kafka-to-PostgreSQL Event Pipeline
// =============================================================================
// The Event Ingestor is the BRIDGE between Kafka (streaming) and PostgreSQL
// (queryable storage). It consumes ALL events from ALL topics and persists
// them to the event store database.
//
// WHY IS THIS NEEDED?
//   Kafka is excellent for streaming but terrible for ad-hoc queries.
//   You can't efficiently ask Kafka:
//     "Show me all events for correlation_id req-123"
//     "What events happened in the last 5 minutes?"
//     "Which services had failures today?"
//
//   PostgreSQL with JSONB gives us:
//     → Rich SQL querying (JOINs, aggregations, full-text search)
//     → Indexed lookups by correlation_id, event_type, timestamp
//     → Timeline reconstruction with ORDER BY timestamp
//     → JSONB payload queries (search within event data)
//
// ARCHITECTURE:
//   Kafka (all topics) → Event Ingestor → PostgreSQL (events table)
//
// CONSUMER GROUP: "event-ingestor-group"
//   - Separate from service consumer groups
//   - Gets its own copy of ALL events (independent offset tracking)
//   - Can be scaled horizontally (more instances = more throughput)
//
// DESIGN DECISIONS:
//   - Uses batch inserts for efficiency (configurable batch size)
//   - Upserts (INSERT ... ON CONFLICT) for idempotent ingestion
//   - Connection pooling for database efficiency
//   - Health endpoint for monitoring
//
// PORT: 3003
// =============================================================================

const express = require('express');
const cors = require('cors');
const { createConsumer, createLogger } = require('@chronoscope/core');
const { createEventStore } = require('./store');

// =============================================================================
// CONFIGURATION
// =============================================================================
const PORT = process.env.INGESTOR_PORT || 3003;
const SERVICE_NAME = 'event-ingestor';

// Topics to consume — we want ALL events from ALL topics
const TOPICS = [
    'orders-events',
    'payments-events',
    'system-events',
];

// =============================================================================
// INITIALIZATION
// =============================================================================
const logger = createLogger(SERVICE_NAME);
const app = express();

app.use(cors({ origin: '*' }));
app.use(express.json());

// =============================================================================
// METRICS — Track ingestion performance
// =============================================================================
// These metrics help monitor the health of the ingestion pipeline.
// In production, export these to Prometheus/Grafana.
// =============================================================================
const metrics = {
    eventsIngested: 0,          // Total events successfully stored
    eventsFailed: 0,            // Total events that failed to store
    lastIngestionTime: null,    // Timestamp of last successful ingestion
    startTime: Date.now(),      // Service start time
};

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get('/health', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'healthy',
        metrics: {
            ...metrics,
            eventsPerSecond: metrics.eventsIngested / ((Date.now() - metrics.startTime) / 1000),
            uptime: process.uptime(),
        },
        timestamp: new Date().toISOString(),
    });
});

// =============================================================================
// METRICS ENDPOINT — Prometheus-compatible metrics
// =============================================================================
app.get('/metrics', (req, res) => {
    res.json(metrics);
});

// =============================================================================
// SERVICE STARTUP
// =============================================================================
async function start() {
    try {
        logger.info('Starting Event Ingestor...');

        // -----------------------------------------------------------------
        // Step 1: Initialize Event Store (PostgreSQL connection)
        // -----------------------------------------------------------------
        // The store handles all database operations:
        //   - Connection pooling
        //   - Insert/upsert operations
        //   - Query operations (for debug API)
        // -----------------------------------------------------------------
        const store = createEventStore();
        await store.connect();
        logger.info('PostgreSQL Event Store connected');

        // -----------------------------------------------------------------
        // Step 2: Create Kafka consumer and subscribe to ALL topics
        // -----------------------------------------------------------------
        // We use fromBeginning: true to ensure we capture ALL events,
        // even if the ingestor starts after events have been produced.
        //
        // IMPORTANT: The consumer group "event-ingestor-group" is separate
        // from service groups. This means the ingestor gets its OWN copy
        // of every event, independent of service processing.
        // -----------------------------------------------------------------
        const consumer = createConsumer(SERVICE_NAME, 'event-ingestor-group');
        await consumer.subscribe(TOPICS, true); // fromBeginning: true
        logger.info('Subscribed to topics', { topics: TOPICS });

        // -----------------------------------------------------------------
        // Step 3: Start consuming and storing events
        // -----------------------------------------------------------------
        // For each event received from Kafka:
        //   1. Enrich with Kafka metadata (topic, partition, offset)
        //   2. Upsert into PostgreSQL (idempotent — handles duplicates)
        //   3. Update metrics
        //
        // WHY UPSERT (INSERT ... ON CONFLICT)?
        //   - If the ingestor restarts, it re-reads from the last
        //     committed offset (or from the beginning)
        //   - Some events might be re-read (at-least-once delivery)
        //   - The upsert ensures duplicates are silently ignored
        //   - event_id is the primary key, so duplicates are detected
        // -----------------------------------------------------------------
        await consumer.start(async (event, metadata) => {
            try {
                // Enrich event with Kafka metadata
                // This is useful for debugging the ingestion pipeline itself
                const enrichedEvent = {
                    ...event,
                    metadata: {
                        ...event.metadata,
                        kafka_topic: metadata.topic,
                        kafka_partition: metadata.partition,
                        kafka_offset: metadata.offset,
                        ingested_at: new Date().toISOString(),
                    },
                };

                // Store in PostgreSQL (idempotent upsert)
                await store.insertEvent(enrichedEvent);

                // Update metrics
                metrics.eventsIngested++;
                metrics.lastIngestionTime = new Date().toISOString();

                logger.info('Event ingested', {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    correlation_id: event.correlation_id,
                    service: event.service,
                    topic: metadata.topic,
                    partition: metadata.partition,
                    offset: metadata.offset,
                    total_ingested: metrics.eventsIngested,
                });

            } catch (error) {
                metrics.eventsFailed++;

                logger.error('Failed to ingest event', {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    error: error.message,
                    total_failed: metrics.eventsFailed,
                });

                // Don't rethrow — we don't want one bad event to stop ingestion
                // In production, you'd send failed events to a dead-letter queue
            }
        });

        logger.info('Event Ingestor consumer started');

        // -----------------------------------------------------------------
        // Step 4: Start Express server
        // -----------------------------------------------------------------
        app.listen(PORT, () => {
            logger.info(`Event Ingestor running on port ${PORT}`, {
                port: PORT,
                topics: TOPICS,
                endpoints: [
                    `GET http://localhost:${PORT}/health`,
                    `GET http://localhost:${PORT}/metrics`,
                ],
            });
        });

        // -----------------------------------------------------------------
        // Graceful Shutdown
        // -----------------------------------------------------------------
        const shutdown = async (signal) => {
            logger.info(`${signal} received — shutting down...`);
            await consumer.disconnect();
            await store.disconnect();
            logger.info('Event Ingestor stopped', {
                totalIngested: metrics.eventsIngested,
                totalFailed: metrics.eventsFailed,
            });
            process.exit(0);
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));

    } catch (error) {
        logger.error('Failed to start Event Ingestor', {
            error: error.message,
            stack: error.stack,
        });
        process.exit(1);
    }
}

start();
