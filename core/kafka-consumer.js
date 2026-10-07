// =============================================================================
// KAFKA CONSUMER — Centralized Event Consumption Utility
// =============================================================================
// This module provides a reusable Kafka consumer that services use to
// subscribe to event topics. Centralizing consumer logic ensures:
//
//   1. CONSISTENT DESERIALIZATION — All events are parsed identically
//   2. ERROR HANDLING — Failed messages are logged and optionally retried
//   3. OFFSET MANAGEMENT — Commits are handled automatically after processing
//   4. GRACEFUL SHUTDOWN — Consumers disconnect cleanly on SIGTERM/SIGINT
//
// CONSUMER GROUP STRATEGY:
//   - Each service gets its own consumer group (e.g., "payment-service-group")
//   - This means each service gets ALL events from subscribed topics
//   - Within a group, Kafka distributes partitions across instances
//   - If a service has 3 instances and a topic has 3 partitions,
//     each instance processes one partition (horizontal scaling)
//
// OFFSET MANAGEMENT:
//   - We use manual commit after successful processing
//   - If processing fails, the offset is NOT committed
//   - On restart, the consumer re-reads from the last committed offset
//   - This gives us AT-LEAST-ONCE delivery (duplicates possible)
//   - Combined with idempotency checks, we achieve EFFECTIVELY-ONCE

// =============================================================================

const { Kafka, logLevel } = require('kafkajs');
const { validateEvent } = require('./event-schema');
const { createLogger } = require('./logger');

// =============================================================================
// CONFIGURATION
// =============================================================================
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// =============================================================================
// createConsumer() — Factory function for creating a Kafka event consumer
// =============================================================================
// Creates a configured Kafka consumer for a specific service and group.
//
// PARAMETERS:
//   @param {string} serviceName — Name of the consuming service
//   @param {string} groupId    — Consumer group ID (determines message distribution)
//
// RETURNS:
//   An object with subscribe(), start(), and disconnect() methods
//
// EXAMPLE:
//   const consumer = createConsumer('payment-service', 'payment-service-group');
//   await consumer.subscribe(['orders-events']);
//   await consumer.start(async (event) => {
//       // Process the event
//       await handleOrderCreated(event);
//   });
// =============================================================================
function createConsumer(serviceName, groupId) {
    const logger = createLogger(serviceName);

    // -------------------------------------------------------------------------
    // Initialize Kafka client
    // -------------------------------------------------------------------------
    const kafka = new Kafka({
        clientId: `${serviceName}-consumer`,
        brokers: [KAFKA_BROKER],
        retry: {
            initialRetryTime: 300,
            retries: 10,
            maxRetryTime: 30000,
            factor: 2,
        },
        logLevel: logLevel.WARN,
    });

    // -------------------------------------------------------------------------
    // Create the consumer instance
    // -------------------------------------------------------------------------
    // SESSION TIMEOUT: If the consumer doesn't send a heartbeat within this
    // time, Kafka considers it dead and reassigns its partitions.
    //
    // HEARTBEAT INTERVAL: How often the consumer sends heartbeats.
    // Rule of thumb: heartbeatInterval = sessionTimeout / 3
    //
    // MAX WAIT TIME: How long the consumer waits for new messages.
    // Lower = more responsive, Higher = more efficient batching.
    // -------------------------------------------------------------------------
    const consumer = kafka.consumer({
        groupId: groupId || `${serviceName}-group`,
        sessionTimeout: 30000,           // 30s session timeout
        heartbeatInterval: 10000,        // 10s heartbeat interval
        maxWaitTimeInMs: 5000,           // 5s max wait for new messages
        // Start from earliest offset for new groups (important for replay)
        // This means new consumers will read ALL historical messages
        // For existing groups, Kafka remembers the last committed offset

        // -------------------------------------------------------------------
        // SELF-HEAL ON CRASH
        // -------------------------------------------------------------------
        // On a single-broker dev cluster, a consumer that joins before the
        // group coordinator / __consumer_offsets topic is ready can crash
        // with "group coordinator is not available". The HTTP server stays
        // up, so Docker's restart policy never fires and the service sits
        // there healthy-looking but not consuming.
        //
        // restartOnFailure MUST live here in the consumer config — kafkajs
        // ignores a `retry` key passed to consumer.run() (that was the bug:
        // event-ingestor crashed at cold boot and never recovered).
        // -------------------------------------------------------------------
        retry: {
            initialRetryTime: 300,
            retries: 10,
            maxRetryTime: 30000,
            factor: 2,
            restartOnFailure: async (error) => {
                logger.warn('Consumer crashed — restarting consumer loop', {
                    error: error.message,
                });
                return true;
            },
        },
    });

    // Track subscribed topics and connection state
    let isConnected = false;
    let subscribedTopics = [];

    return {
        // =====================================================================
        // subscribe() — Subscribe to one or more Kafka topics
        // =====================================================================
        // Must be called BEFORE start(). Can subscribe to multiple topics.
        //
        // fromBeginning: true → Read all historical messages (for new groups)
        // fromBeginning: false → Read only new messages from this point
        //
        // IMPORTANT: For the event-ingestor, we use fromBeginning: true
        // to ensure we capture ALL events, even if the ingestor starts
        // after events have been produced.
        // =====================================================================
        async subscribe(topics, fromBeginning = true) {
            const connectWithRetry = async (retries = 10) => {
                try {
                    await consumer.connect();
                    isConnected = true;
                } catch (err) {
                    if (retries > 0) {
                        logger.warn(`Kafka connection failed, retrying in 2s... (${retries} retries left)`, { error: err.message });
                        await new Promise(resolve => setTimeout(resolve, 2000));
                        return connectWithRetry(retries - 1);
                    }
                    throw err;
                }
            };

            try {
                if (!isConnected) await connectWithRetry();

                // Subscribe to each topic
                for (const topic of topics) {
                    await consumer.subscribe({
                        topic,
                        fromBeginning,
                    });
                    subscribedTopics.push(topic);
                    logger.info(`Subscribed to topic: ${topic}`, { fromBeginning });
                }
            } catch (error) {
                logger.error('Failed to subscribe', {
                    topics,
                    error: error.message,
                });
                throw error;
            }
        },

        // =====================================================================
        // start() — Begin consuming messages and processing them
        // =====================================================================
        // This is the main event loop. It reads messages from Kafka,
        // deserializes them, validates the schema, and calls your handler.
        //
        // PARAMETERS:
        //   @param {Function} handler — Async function called for each event
        //                               Signature: handler(event, metadata)
        //   @param {object} options   — Optional configuration
        //     - autoCommit: boolean   → Auto-commit offsets (default: false)
        //     - batchSize: number     → Messages per batch (default: 10)
        //
        // FLOW:
        //   1. Read message batch from Kafka
        //   2. For each message:
        //      a. Deserialize JSON → event object
        //      b. Validate against schema
        //      c. Call handler(event, metadata)
        //      d. If handler succeeds → commit offset
        //      e. If handler fails → log error, don't commit (message will be re-read)
        //
        // ERROR HANDLING:
        //   - Deserialization errors → logged and skipped (committed to avoid infinite loop)
        //   - Validation errors → logged and skipped
        //   - Handler errors → logged, NOT committed (message will be retried)
        // =====================================================================
        async start(handler, options = {}) {
            if (!isConnected) {
                throw new Error('Consumer not connected. Call subscribe() first.');
            }

            const { autoCommit = false } = options;

            logger.info('Starting consumer event loop', {
                topics: subscribedTopics,
                groupId: groupId || `${serviceName}-group`,
                autoCommit,
            });

            await consumer.run({
                // Manual offset management for at-least-once delivery
                autoCommit,

                // (Self-heal restartOnFailure lives in the kafka.consumer()
                // config above — kafkajs ignores a retry key here in run().)

                // ---------------------------------------------------------------
                // MESSAGE HANDLER
                // ---------------------------------------------------------------
                // Called for each message received from Kafka.
                // The message contains: topic, partition, offset, key, value, headers
                // ---------------------------------------------------------------
                eachMessage: async ({ topic, partition, message }) => {
                    const messageMetadata = {
                        topic,
                        partition,
                        offset: message.offset,
                        key: message.key?.toString(),
                        timestamp: message.timestamp,
                    };

                    try {
                        // -------------------------------------------------
                        // Step 1: Deserialize the message
                        // -------------------------------------------------
                        // Kafka stores messages as byte arrays.
                        // We assume JSON encoding (configured by producer).
                        // -------------------------------------------------
                        const rawValue = message.value.toString();
                        let event;

                        try {
                            event = JSON.parse(rawValue);
                        } catch (parseError) {
                            // Message is not valid JSON — skip it
                            // We DO commit this offset to avoid stuck consumer
                            logger.error('Failed to parse message — skipping', {
                                ...messageMetadata,
                                error: parseError.message,
                                rawValue: rawValue.substring(0, 200), // Truncate for logging
                            });

                            // Commit the bad message's offset to move past it
                            if (!autoCommit) {
                                await consumer.commitOffsets([{
                                    topic,
                                    partition,
                                    offset: (parseInt(message.offset) + 1).toString(),
                                }]);
                            }
                            return;
                        }

                        // -------------------------------------------------
                        // Step 2: Validate the event schema
                        // -------------------------------------------------
                        // Ensures the deserialized object has all required
                        // fields. Prevents handler from crashing on bad data.
                        // -------------------------------------------------
                        const validation = validateEvent(event);
                        if (!validation.valid) {
                            logger.warn('Event validation failed — skipping', {
                                ...messageMetadata,
                                event_id: event.event_id,
                                errors: validation.errors,
                            });

                            // Commit to avoid re-processing invalid events
                            if (!autoCommit) {
                                await consumer.commitOffsets([{
                                    topic,
                                    partition,
                                    offset: (parseInt(message.offset) + 1).toString(),
                                }]);
                            }
                            return;
                        }

                        // -------------------------------------------------
                        // Step 3: Call the handler
                        // -------------------------------------------------
                        // The handler is provided by the consuming service.
                        // It receives the validated event and message metadata.
                        //
                        // If the handler throws, we DON'T commit the offset,
                        // so the message will be re-read on next poll.
                        // -------------------------------------------------
                        logger.debug('Processing event', {
                            event_id: event.event_id,
                            event_type: event.event_type,
                            correlation_id: event.correlation_id,
                            ...messageMetadata,
                        });

                        await handler(event, messageMetadata);

                        // -------------------------------------------------
                        // Step 4: Commit offset after successful processing
                        // -------------------------------------------------
                        // Only commit AFTER the handler succeeds.
                        // This ensures at-least-once delivery:
                        //   - If handler succeeds → offset committed → message won't be re-read
                        //   - If handler fails → offset NOT committed → message re-read
                        //   - If commit fails after success → message re-read (duplicate)
                        //     → idempotency checks handle this case
                        // -------------------------------------------------
                        if (!autoCommit) {
                            await consumer.commitOffsets([{
                                topic,
                                partition,
                                offset: (parseInt(message.offset) + 1).toString(),
                            }]);
                        }

                        logger.debug('Event processed successfully', {
                            event_id: event.event_id,
                            event_type: event.event_type,
                        });

                    } catch (error) {
                        // -------------------------------------------------
                        // Handler failed — DO NOT commit offset
                        // -------------------------------------------------
                        // The message will be re-read from Kafka on next poll.
                        // This provides automatic retry behavior.
                        //
                        // WARNING: If the handler consistently fails for a
                        // specific message, the consumer will get stuck.
                        // In production, implement a dead-letter queue (DLQ)
                        // to move poison messages out of the main flow.
                        // -------------------------------------------------
                        logger.error('Event processing failed', {
                            ...messageMetadata,
                            error: error.message,
                            stack: error.stack,
                        });

                        // In a production system, you'd implement:
                        // 1. Retry counter (max 3 retries)
                        // 2. Dead-letter queue (DLQ) for poison messages
                        // 3. Circuit breaker for downstream service failures
                    }
                },
            });
        },

        // =====================================================================
        // disconnect() — Gracefully disconnect from Kafka
        // =====================================================================
        // Flushes pending commits and releases partition assignments.
        // ALWAYS call this during shutdown to prevent partition lag.
        // =====================================================================
        async disconnect() {
            try {
                await consumer.disconnect();
                isConnected = false;
                logger.info('Consumer disconnected', {
                    topics: subscribedTopics,
                });
            } catch (error) {
                logger.error('Failed to disconnect consumer', {
                    error: error.message,
                });
            }
        },

        // =====================================================================
        // seekToBeginning() — Reset consumer to read from the beginning
        // =====================================================================
        // Used by the replay engine to re-read all events from Kafka.
        // This is different from subscribing with fromBeginning — this
        // resets an EXISTING consumer group's offsets.
        // =====================================================================
        async seekToBeginning(topic, partition = 0) {
            await consumer.seek({
                topic,
                partition,
                offset: '0',
            });
            logger.info('Consumer seeked to beginning', { topic, partition });
        },
    };
}

module.exports = { createConsumer };
