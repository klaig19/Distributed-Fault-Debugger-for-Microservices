// =============================================================================
// PAYMENT SERVICE — Entry Point
// =============================================================================
// The Payment Service is an EVENT-DRIVEN service. Unlike the Order Service
// (which is primarily REST-driven), this service is triggered by Kafka events.
//
// PRIMARY TRIGGER:
//   ORDER_CREATED event from orders-events topic
//     → Initiates payment processing
//     → Emits PAYMENT_INITIATED event
//     → Simulates payment processing (with configurable delay + failure rate)
//     → Emits PAYMENT_SUCCESS or PAYMENT_FAILED event
//
// ARCHITECTURE ROLE:
//   This service demonstrates the CONSUMER side of the event-driven architecture.
//   It consumes events from Kafka, processes them, and emits new events:
//     Kafka (ORDER_CREATED) → Payment Service → Kafka (PAYMENT_SUCCESS/FAILED)
//
// SIDE-EFFECT ISOLATION (CRITICAL FOR REPLAY):
//   This service contains side effects:
//     - External payment gateway calls (simulated)
//     - Email notifications (simulated)
//   During replay, these MUST be disabled.
//
//   The REPLAY_MODE environment variable controls this:
//     REPLAY_MODE=true  → Skip external calls, only process state
//     REPLAY_MODE=false → Normal operation with all side effects
//
//   WHY? If you replay events without disabling side effects, you'll:
//     - Double-charge customers
//     - Send duplicate emails
//     - Trigger duplicate inventory shipments
//     - Corrupt external system state
//
// PORT: 3002 (for health check and admin endpoints)
// =============================================================================

const express = require('express');
const cors = require('cors');
const {
    createProducer,
    createConsumer,
    createLogger,
    EventTypes,
} = require('@chronoscope/core');
const { createPaymentHandler } = require('./handlers/payment-handler');

// =============================================================================
// CONFIGURATION
// =============================================================================
const PORT = process.env.PAYMENT_SERVICE_PORT || 3002;
const SERVICE_NAME = 'payment-service';

// REPLAY MODE FLAG — Controls side-effect isolation
// When true, external calls (payment gateway, email) are SKIPPED
// This is the KEY to safe replay
const REPLAY_MODE = process.env.REPLAY_MODE === 'true';

// =============================================================================
// INITIALIZATION
// =============================================================================
const logger = createLogger(SERVICE_NAME);
const app = express();

// Middleware
app.use(cors({ origin: '*' }));
app.use(express.json());

// =============================================================================
// HEALTH CHECK
// =============================================================================
app.get('/health', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'healthy',
        replayMode: REPLAY_MODE,
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
    });
});

// =============================================================================
// SERVICE STARTUP
// =============================================================================
// Startup sequence:
//   1. Initialize Kafka producer (for emitting payment events)
//   2. Initialize Kafka consumer (for receiving order events)
//   3. Create payment handler (with producer + replay mode flag)
//   4. Subscribe to orders-events topic
//   5. Start consuming and processing events
//   6. Start Express server (for health checks)
//
// CONSUMER GROUP: "payment-service-group"
//   - All instances of payment-service share this group
//   - Kafka distributes partitions across instances
//   - If one instance dies, its partitions are reassigned to others
// =============================================================================
async function start() {
    try {
        logger.info('Starting Payment Service...', { replayMode: REPLAY_MODE });

        if (REPLAY_MODE) {
            logger.warn('REPLAY MODE ENABLED — Side effects are DISABLED', {
                disabledEffects: [
                    'Payment gateway calls',
                    'Email notifications',
                    'SMS notifications',
                    'Webhook triggers',
                ],
            });
        }

        // -----------------------------------------------------------------
        // Step 1: Connect Kafka producer
        // -----------------------------------------------------------------
        const producer = createProducer(SERVICE_NAME);
        await producer.connect();
        logger.info('Kafka producer connected');

        // -----------------------------------------------------------------
        // Step 2: Initialize payment handler
        // -----------------------------------------------------------------
        // The handler receives the REPLAY_MODE flag to control side effects.
        // This is DEPENDENCY INJECTION for the replay behavior.
        // The handler doesn't decide if it's in replay mode — the caller does.
        // -----------------------------------------------------------------
        const paymentHandler = createPaymentHandler(producer, SERVICE_NAME, {
            replayMode: REPLAY_MODE,
            // Simulated payment settings
            paymentDelayMs: 1000,      // Simulate 1s payment processing
            failureRate: 0.1,          // 10% chance of payment failure
        });

        // -----------------------------------------------------------------
        // Step 3: Create Kafka consumer and subscribe to orders-events
        // -----------------------------------------------------------------
        // We listen for ORDER_CREATED events to trigger payment processing.
        // The consumer group ensures each event is processed ONCE across
        // all payment-service instances.
        // -----------------------------------------------------------------
        const consumer = createConsumer(SERVICE_NAME, 'payment-service-group');
        await consumer.subscribe(['orders-events']);
        logger.info('Subscribed to orders-events topic');

        // -----------------------------------------------------------------
        // Step 4: Start consuming events
        // -----------------------------------------------------------------
        // The event handler routes events to the appropriate handler method
        // based on the event_type field.
        //
        // ROUTING:
        //   ORDER_CREATED   → paymentHandler.onOrderCreated(event)
        //   ORDER_CANCELLED → paymentHandler.onOrderCancelled(event)
        //   (other types)   → logged and skipped
        // -----------------------------------------------------------------
        await consumer.start(async (event, metadata) => {
            logger.info('Received event', {
                event_type: event.event_type,
                event_id: event.event_id,
                correlation_id: event.correlation_id,
                topic: metadata.topic,
                partition: metadata.partition,
                offset: metadata.offset,
            });

            // Route event to appropriate handler.
            // Errors are caught per-event: an uncaught throw here would
            // propagate into the kafkajs eachMessage loop and stall this
            // partition in a crash/retry cycle. Matching order-service and
            // event-ingestor semantics: log the failure and move on.
            try {
                switch (event.event_type) {
                case EventTypes.ORDER_CREATED:
                    // ---------------------------------------------------
                    // ORDER_CREATED → Process payment
                    // ---------------------------------------------------
                    // This is the primary event handler. When a new order
                    // is created, we initiate payment processing.
                    //
                    // Flow:
                    //   1. Extract order details from event payload
                    //   2. Emit PAYMENT_INITIATED event
                    //   3. Call payment gateway (or simulate)
                    //   4. Emit PAYMENT_SUCCESS or PAYMENT_FAILED event
                    // ---------------------------------------------------
                    await paymentHandler.onOrderCreated(event);
                    break;

                case EventTypes.ORDER_CANCELLED:
                    // ---------------------------------------------------
                    // ORDER_CANCELLED → Refund payment
                    // ---------------------------------------------------
                    // If an order is cancelled after payment, initiate refund.
                    // ---------------------------------------------------
                    await paymentHandler.onOrderCancelled(event);
                    break;

                default:
                    // Unknown event type — log and skip
                    // This is normal — we only handle specific event types
                    logger.debug('Skipping unhandled event type', {
                        event_type: event.event_type,
                        event_id: event.event_id,
                    });
                    break;
                }
            } catch (error) {
                logger.error('Failed to process event — skipping to avoid blocking partition', {
                    event_type: event.event_type,
                    event_id: event.event_id,
                    correlation_id: event.correlation_id,
                    error: error.message,
                    stack: error.stack,
                });
            }
        });

        logger.info('Payment Service event consumer started');

        // -----------------------------------------------------------------
        // Step 5: Start Express server (health checks only)
        // -----------------------------------------------------------------
        app.listen(PORT, () => {
            logger.info(`Payment Service running on port ${PORT}`, {
                port: PORT,
                replayMode: REPLAY_MODE,
                endpoints: [
                    `GET http://localhost:${PORT}/health`,
                ],
                consuming: ['orders-events'],
                producing: ['payments-events'],
            });
        });

        // -----------------------------------------------------------------
        // Graceful Shutdown
        // -----------------------------------------------------------------
        const shutdown = async (signal) => {
            logger.info(`${signal} received — shutting down...`);
            await consumer.disconnect();
            await producer.disconnect();
            logger.info('Payment Service stopped');
            process.exit(0);
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));

    } catch (error) {
        logger.error('Failed to start Payment Service', {
            error: error.message,
            stack: error.stack,
        });
        process.exit(1);
    }
}

start();
