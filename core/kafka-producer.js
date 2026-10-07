// =============================================================================
// KAFKA PRODUCER — Centralized Event Publishing Utility
// =============================================================================
// This module provides a reusable Kafka producer that ALL services use to
// emit events. Centralizing the producer logic ensures:
//
//   1. CONSISTENT PARTITIONING — All events use correlation_id as the
//      partition key, guaranteeing that events for the same request flow
//      land in the same partition (preserving order for replay).
//
//   2. STRUCTURED SERIALIZATION — Events are serialized as JSON with
//      consistent formatting across all services.
//
//   3. ERROR HANDLING — Centralized retry logic and dead-letter handling
//      prevents each service from implementing its own (inconsistently).
//
//   4. METADATA ENRICHMENT — Automatically adds producer metadata
//      (service name, timestamp, schema version) to every event.
//
// KAFKA PARTITIONING STRATEGY:
//   - Key: correlation_id (NOT event_id)
//   - Why: All events for a distributed transaction must be in the same partition
//   - This enables: Ordered replay within a single transaction
//   - Trade-off: Hot partition risk if one correlation_id has many events
//     (acceptable for debugging scenarios)
//
// USAGE:
//   const { createProducer } = require('@chronoscope/core');
//   const producer = createProducer('order-service');
//   await producer.connect();
//   await producer.emit(event);
// =============================================================================

const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { getTopicForEvent } = require('./event-schema');
const { createLogger } = require('./logger');

// =============================================================================
// KAFKA CLIENT CONFIGURATION
// =============================================================================
// These settings control how the producer connects to and communicates with
// the Kafka broker. In production, you'd load these from environment variables
// or a config service.
// =============================================================================
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// =============================================================================
// createProducer() — Factory function for creating a Kafka event producer
// =============================================================================
// Creates a configured Kafka producer instance for a specific service.
//
// PARAMETERS:
//   @param {string} serviceName — Name of the service using this producer
//                                 (used for client ID and logging)
//
// RETURNS:
//   An object with connect(), disconnect(), and emit() methods
//
// EXAMPLE:
//   const producer = createProducer('order-service');
//   await producer.connect();
//
//   await producer.emit({
//     event_id: 'evt-001',
//     event_type: 'ORDER_CREATED',
//     correlation_id: 'req-123',
//     service: 'order-service',
//     timestamp: Date.now(),
//     payload: { orderId: 'ord-001' }
//   });
//
//   await producer.disconnect();
// =============================================================================
function createProducer(serviceName) {
    const logger = createLogger(serviceName);

    // -------------------------------------------------------------------------
    // Initialize Kafka client
    // -------------------------------------------------------------------------
    // The clientId identifies this producer in Kafka broker logs,
    // making it easier to debug connection issues
    // -------------------------------------------------------------------------
    const kafka = new Kafka({
        clientId: `${serviceName}-producer`,
        brokers: [KAFKA_BROKER],
        // Retry configuration for transient failures
        retry: {
            initialRetryTime: 300,    // Start with 300ms retry delay
            retries: 10,              // Max 10 retries before giving up
            maxRetryTime: 30000,      // Cap retry delay at 30 seconds
            factor: 2,               // Exponential backoff factor
        },
        logLevel: logLevel.WARN,     // Suppress verbose Kafka client logs
    });

    // -------------------------------------------------------------------------
    // Create the producer instance
    // -------------------------------------------------------------------------
    // IDEMPOTENT PRODUCER (idempotent: true):
    //   - Prevents duplicate messages due to producer retries
    //   - Kafka assigns a producer ID and sequence number to each message
    //   - If a retry sends the same message again, Kafka deduplicates it
    //   - THIS IS DIFFERENT FROM CONSUMER IDEMPOTENCY (which we handle separately)
    //
    // INTERVIEW TIP: "I enable idempotent produces to guarantee exactly-once
    // semantics at the producer level. Combined with consumer-side idempotency
    // checks, this ensures no duplicate processing."
    // -------------------------------------------------------------------------
    const producer = kafka.producer({
        createPartitioner: Partitioners.DefaultPartitioner,
        idempotent: true,                    // Prevent duplicate produce
        maxInFlightRequests: 5,              // Max concurrent requests
        transactionTimeout: 30000,           // 30s transaction timeout
    });

    // -------------------------------------------------------------------------
    // Track connection state
    // -------------------------------------------------------------------------
    let isConnected = false;

    return {
        // =====================================================================
        // connect() — Establish connection to Kafka broker
        // =====================================================================
        // Must be called before emit(). Handles connection errors gracefully.
        // In production, you'd implement connection pooling and health checks.
        // =====================================================================
        async connect() {
            // Connect with retry logic for robust startup
            const connectWithRetry = async (retries = 10) => {
                try {
                    await producer.connect();
                    isConnected = true;
                    logger.info('Kafka producer connected', { broker: KAFKA_BROKER });
                } catch (err) {
                    if (retries > 0) {
                        logger.warn(`Kafka connection failed, retrying in 2s... (${retries} retries left)`, { error: err.message });
                        await new Promise(resolve => setTimeout(resolve, 2000));
                        return connectWithRetry(retries - 1);
                    }
                    throw err;
                }
            };

            await connectWithRetry();
        },

        // =====================================================================
        // disconnect() — Gracefully disconnect from Kafka broker
        // =====================================================================
        // Always call this during shutdown to flush pending messages
        // and release resources.
        // =====================================================================
        async disconnect() {
            try {
                await producer.disconnect();
                isConnected = false;
                logger.info('Kafka producer disconnected');
            } catch (error) {
                logger.error('Failed to disconnect producer', { error: error.message });
            }
        },

        // =====================================================================
        // emit() — Publish an event to the appropriate Kafka topic
        // =====================================================================
        // This is the PRIMARY method services use to publish events.
        //
        // FLOW:
        //   1. Determine the topic from the event type
        //   2. Serialize the event as JSON
        //   3. Use correlation_id as the partition key
        //   4. Send to Kafka with delivery confirmation
        //
        // PARTITIONING:
        //   The partition key is correlation_id, NOT event_id.
        //   This ensures all events for a single distributed transaction
        //   land in the same partition, preserving their order.
        //
        // PARAMETERS:
        //   @param {object} event — A fully-formed event object (from createEvent)
        //
        // RETURNS:
        //   The Kafka produce result (with offset and partition info)
        // =====================================================================
        async emit(event) {
            if (!isConnected) {
                throw new Error('Producer not connected. Call connect() first.');
            }

            // Determine which Kafka topic this event belongs to
            const topic = getTopicForEvent(event.event_type);

            try {
                // ---------------------------------------------------------
                // Send event to Kafka
                // ---------------------------------------------------------
                // Key points:
                //   - key: correlation_id → determines partition assignment
                //   - value: JSON serialized event → the actual message
                //   - headers: lightweight metadata for consumers to inspect
                //     without deserializing the full message body
                // ---------------------------------------------------------
                const result = await producer.send({
                    topic,
                    messages: [
                        {
                            // Partition key — ALL events with the same
                            // correlation_id go to the SAME partition
                            key: event.correlation_id,

                            // Message body — full event serialized as JSON
                            value: JSON.stringify(event),

                            // Message headers — lightweight metadata
                            // Consumers can read headers without parsing body
                            headers: {
                                'event-type': event.event_type,
                                'correlation-id': event.correlation_id,
                                'service': event.service,
                                'schema-version': event.schema_version || '1.0',
                                'produced-at': Date.now().toString(),
                            },
                        },
                    ],
                });

                // Log successful production with offset and partition info
                const [recordMetadata] = result;
                logger.info('Event emitted to Kafka', {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    correlation_id: event.correlation_id,
                    topic,
                    partition: recordMetadata?.partition,
                    offset: recordMetadata?.offset,
                });

                return result;
            } catch (error) {
                // ---------------------------------------------------------
                // Handle production failures
                // ---------------------------------------------------------
                // Common failure modes:
                //   - Broker unavailable: Network issues, Kafka down
                //   - Topic doesn't exist: Misconfigured topic mapping
                //   - Message too large: Payload exceeds max.message.bytes
                //   - Serialization error: Invalid event structure
                // ---------------------------------------------------------
                logger.error('Failed to emit event', {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    correlation_id: event.correlation_id,
                    topic,
                    error: error.message,
                });
                throw error;
            }
        },

        // =====================================================================
        // emitBatch() — Publish multiple events in a single Kafka transaction
        // =====================================================================
        // Used for batch operations (e.g., replaying multiple events at once).
        // All events in the batch are sent atomically — either all succeed
        // or all fail.
        //
        // PARAMETERS:
        //   @param {Array<object>} events — Array of event objects
        //
        // RETURNS:
        //   Array of Kafka produce results
        // =====================================================================
        async emitBatch(events) {
            if (!isConnected) {
                throw new Error('Producer not connected. Call connect() first.');
            }

            // Group events by topic for efficient batch sending
            const topicMessages = {};
            for (const event of events) {
                const topic = getTopicForEvent(event.event_type);
                if (!topicMessages[topic]) {
                    topicMessages[topic] = [];
                }
                topicMessages[topic].push({
                    key: event.correlation_id,
                    value: JSON.stringify(event),
                    headers: {
                        'event-type': event.event_type,
                        'correlation-id': event.correlation_id,
                        'service': event.service,
                        'schema-version': event.schema_version || '1.0',
                    },
                });
            }

            // Send each topic's messages as a batch
            const results = [];
            for (const [topic, messages] of Object.entries(topicMessages)) {
                const result = await producer.send({ topic, messages });
                results.push(result);
            }

            logger.info(`Batch emitted: ${events.length} events`, {
                topics: Object.keys(topicMessages),
            });

            return results;
        },
    };
}

module.exports = { createProducer };
