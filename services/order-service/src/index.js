// =============================================================================
// ORDER SERVICE — Entry Point
// =============================================================================
// The Order Service is the PRIMARY entry point for user interactions.
// It handles order lifecycle management:
//   - Creating new orders
//   - Updating existing orders
//   - Cancelling orders
//   - Querying order status
//
// EVENT PRODUCTION:
//   Every state change emits a structured event to Kafka:
//     POST /orders → ORDER_CREATED event → orders-events topic
//     PUT /orders/:id → ORDER_UPDATED event → orders-events topic
//     DELETE /orders/:id → ORDER_CANCELLED event → orders-events topic
//
// ARCHITECTURE ROLE:
//   This service sits at the beginning of the event chain:
//     Client → Order Service → Kafka → Payment Service → ...
//   It's the "source of truth" for order state and the first event producer.
//
// PORT: 3001 (configurable via ORDER_SERVICE_PORT env var)
//
// DESIGN DECISIONS:
//   - Uses PostgreSQL for order state (CRUD operations)
//   - Emits events AFTER successful DB write (not before)
//   - Uses correlation_id middleware for request tracing
//   - Every response includes the correlation_id for client-side tracing
// =============================================================================

const express = require('express');
const cors = require('cors');
const {
    correlationMiddleware,
    createProducer,
    createConsumer,
    createLogger,
    EventTypes,
} = require('@chronoscope/core');
const { createOrderRoutes } = require('./routes/orders');
const { createOrderHandler } = require('./handlers/order-handler');

// =============================================================================
// CONFIGURATION
// =============================================================================
// All configuration is loaded from environment variables with sensible defaults.
// In production, these would come from a config service or secrets manager.
// =============================================================================
const PORT = process.env.ORDER_SERVICE_PORT || 3001;
const SERVICE_NAME = 'order-service';

// =============================================================================
// INITIALIZATION
// =============================================================================
const logger = createLogger(SERVICE_NAME);
const app = express();

// =============================================================================
// MIDDLEWARE STACK
// =============================================================================
// Order matters! Middleware executes in the order it's registered.
//   1. CORS       → Allow cross-origin requests (for debug UI)
//   2. JSON       → Parse request bodies as JSON
//   3. Correlation → Generate/propagate correlation IDs
// =============================================================================

// Enable CORS for the Debug UI (running on a different port)
app.use(cors({
    origin: '*',                    // Allow all origins in development
    exposedHeaders: [               // Expose custom headers to the client
        'x-correlation-id',
        'x-request-start',
        'x-source-service',
    ],
}));

// Parse JSON request bodies (with a 10MB limit for large order payloads)
app.use(express.json({ limit: '10mb' }));

// Correlation ID middleware — THE MOST IMPORTANT MIDDLEWARE
// This ensures every request has a correlation_id for distributed tracing
app.use(correlationMiddleware(SERVICE_NAME));

// =============================================================================
// HEALTH CHECK ENDPOINT
// =============================================================================
// Used by:
//   - Docker health checks (HEALTHCHECK directive)
//   - Load balancers (to determine if the service can accept traffic)
//   - Monitoring systems (uptime tracking)
//
// Returns service metadata and health status.
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
// Startup sequence:
//   1. Initialize Kafka producer (connect to broker)
//   2. Initialize order handler (with producer dependency)
//   3. Register order routes
//   4. Start Express server
//
// WHY ASYNC STARTUP?
//   - Kafka connection is async and can fail
//   - We want to fail FAST if infrastructure isn't available
//   - Better than starting the server and failing on first request
// =============================================================================
async function start() {
    try {
        logger.info('Starting Order Service...');

        // -----------------------------------------------------------------
        // Step 1: Connect Kafka producer
        // -----------------------------------------------------------------
        const producer = createProducer(SERVICE_NAME);
        await producer.connect();
        logger.info('Kafka producer connected');

        // -----------------------------------------------------------------
        // Step 2: Initialize Order Handler
        // -----------------------------------------------------------------
        // The handler contains the business logic. We inject the Kafka producer
        // so the handler can emit events without knowing about the network.
        // -----------------------------------------------------------------
        const orderHandler = createOrderHandler(producer, SERVICE_NAME);
        logger.info('Order handler initialized');

        // -----------------------------------------------------------------
        // Step 3: Register Order Routes
        // -----------------------------------------------------------------
        // Attach the routes to the Express app.
        // The routes module uses the handler to execute commands.
        // -----------------------------------------------------------------
        app.use('/orders', createOrderRoutes(orderHandler));
        logger.info('Order routes registered');

        // -----------------------------------------------------------------
        // Step 3.5: Create Kafka consumer to listen to payments-events
        // -----------------------------------------------------------------
        const consumer = createConsumer(SERVICE_NAME, 'order-service-group');
        await consumer.subscribe(['payments-events']);
        logger.info('Subscribed to payments-events topic');

        await consumer.start(async (event, metadata) => {
            try {
                if (event.event_type === EventTypes.PAYMENT_SUCCESS) {
                    logger.info('Received PAYMENT_SUCCESS, updating order to PAID', { orderId: event.payload.orderId });
                    await orderHandler.updateOrder(event.payload.orderId, {
                        status: 'PAID',
                        correlationId: event.correlation_id,
                    });
                } else if (event.event_type === EventTypes.PAYMENT_FAILED) {
                    logger.info('Received PAYMENT_FAILED, cancelling order', { orderId: event.payload.orderId });
                    await orderHandler.cancelOrder(event.payload.orderId, {
                        reason: 'Payment failed: ' + (event.payload.error || 'Unknown error'),
                        correlationId: event.correlation_id,
                    });
                }
            } catch (err) {
                logger.error('Failed to process payment event in Order Service', { error: err.message });
            }
        });
        logger.info('Order Service event consumer started');

        // -----------------------------------------------------------------
        // Step 4: Start Express server
        // -----------------------------------------------------------------
        app.listen(PORT, () => {
            logger.info(`Order Service running on port ${PORT}`, {
                port: PORT,
                kafka_broker: process.env.KAFKA_BROKER || 'localhost:9092',
                endpoints: [
                    `GET    http://localhost:${PORT}/health`,
                    `POST   http://localhost:${PORT}/orders`,
                    `GET    http://localhost:${PORT}/orders`,
                    `GET    http://localhost:${PORT}/orders/:id`,
                    `PUT    http://localhost:${PORT}/orders/:id`,
                    `DELETE http://localhost:${PORT}/orders/:id`,
                ],
            });
        });

        const shutdown = async (signal) => {
            logger.info(`${signal} received -- shutting down gracefully...`);
            await consumer.disconnect();
            await producer.disconnect();
            logger.info('Order Service stopped');
            process.exit(0);
        };

        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));

    } catch (error) {
        logger.error('Failed to start Order Service', {
            error: error.message,
            stack: error.stack,
        });
        process.exit(1);
    }
}

// Start the service
start();
