// =============================================================================
// CORRELATION MIDDLEWARE — Request Flow Tracing Across Services
// =============================================================================
// This middleware is the BACKBONE of the debugging capability.
//
// WHAT IS A CORRELATION ID?
//   A unique identifier that follows a request across ALL services.
//   When a user places an order, one correlation_id tracks the flow:
//     API Gateway → Order Service → Kafka → Payment Service → ...
//
// WHY IS THIS CRITICAL?
//   - Without correlation IDs, debugging a distributed request is like
//     finding a needle in a haystack of unrelated logs
//   - With correlation IDs, you can instantly reconstruct the ENTIRE
//     request flow across all services
//   - This is the foundation for timeline visualization and replay
//
// HOW IT WORKS:
//   1. Client sends request (with or without x-correlation-id header)
//   2. Middleware checks for existing correlation_id
//   3. If missing → generates a new UUID v4
//   4. Attaches correlation_id to:
//      - Request object (for handler access)
//      - Response headers (for client tracing)
//      - Logger context (for structured logging)
//   5. All downstream events use this correlation_id
//
// HEADER NAME: x-correlation-id
//   - Follows the x- prefix convention for custom headers
//   - Some systems use X-Request-ID, traceparent, or trace-id
//   - We use x-correlation-id for clarity
//

// =============================================================================

const { v4: uuidv4 } = require('uuid');
const { createLogger } = require('./logger');

// =============================================================================
// CONSTANTS
// =============================================================================
// Header name for correlation ID propagation across services
const CORRELATION_HEADER = 'x-correlation-id';

// Header name for the request start time (for latency tracking)
const REQUEST_START_HEADER = 'x-request-start';

// Header name for the originating service (for tracing the request origin)
const SOURCE_SERVICE_HEADER = 'x-source-service';

// =============================================================================
// generateCorrelationId() — Generate a new correlation ID
// =============================================================================
// Uses UUID v4 for global uniqueness without coordination.
// Format: "req-" prefix + UUID (e.g., "req-a1b2c3d4-e5f6-7890-abcd-ef1234567890")
// The "req-" prefix makes it instantly identifiable in logs.
// =============================================================================
function generateCorrelationId() {
    return `req-${uuidv4()}`;
}

// =============================================================================
// correlationMiddleware() — Express middleware for correlation ID management
// =============================================================================
// Add this middleware to your Express app BEFORE any route handlers.
// It ensures every request has a correlation_id, whether from the client
// or auto-generated.
//
// USAGE:
//   const { correlationMiddleware } = require('@chronoscope/core');
//   app.use(correlationMiddleware('order-service'));
//
// WHAT IT DOES:
//   1. Extracts or generates correlation_id
//   2. Attaches it to req.correlationId (for route handlers)
//   3. Adds it to response headers (for client-side tracing)
//   4. Logs the request start with correlation context
//   5. Logs the request completion with duration
//
// PARAMETERS:
//   @param {string} serviceName — The name of the service using this middleware
//
// RETURNS:
//   Express middleware function
// =============================================================================
function correlationMiddleware(serviceName) {
    const logger = createLogger(serviceName);

    return (req, res, next) => {
        // ---------------------------------------------------------------------
        // Step 1: Extract or generate correlation ID
        // ---------------------------------------------------------------------
        // Check if the incoming request already has a correlation ID.
        // This happens when:
        //   a) The API gateway generated one
        //   b) Another service is calling this service (service-to-service)
        //   c) The client explicitly set it (for tracing)
        //
        // If no correlation ID exists, generate a new one.
        // This is the "root" of a new request flow.
        // ---------------------------------------------------------------------
        const existingCorrelationId = req.headers[CORRELATION_HEADER];
        const correlationId = existingCorrelationId || generateCorrelationId();

        // Track whether this is a new flow or a continuation
        const isNewFlow = !existingCorrelationId;

        // ---------------------------------------------------------------------
        // Step 2: Attach correlation ID to the request object
        // ---------------------------------------------------------------------
        // This makes it accessible to ALL route handlers downstream.
        // Handlers use it when creating events:
        //   createEvent(EventTypes.ORDER_CREATED, req.correlationId, ...)
        // ---------------------------------------------------------------------
        req.correlationId = correlationId;

        // Also attach the source service header if present
        req.sourceService = req.headers[SOURCE_SERVICE_HEADER] || 'client';

        // Record request start time for latency measurement
        req.requestStartTime = Date.now();

        // ---------------------------------------------------------------------
        // Step 3: Add correlation ID to response headers
        // ---------------------------------------------------------------------
        // This allows the CLIENT to trace their request through the system.
        // They can use this ID to:
        //   - Query the debug API for the full event timeline
        //   - Report issues with the correlation ID for faster debugging
        //   - Correlate client-side and server-side logs
        // ---------------------------------------------------------------------
        res.setHeader(CORRELATION_HEADER, correlationId);
        res.setHeader(REQUEST_START_HEADER, req.requestStartTime.toString());
        res.setHeader(SOURCE_SERVICE_HEADER, serviceName);

        // ---------------------------------------------------------------------
        // Step 4: Log the incoming request
        // ---------------------------------------------------------------------
        // Structured logging with correlation context enables log aggregation
        // tools (ELK, Datadog, etc.) to group all logs for a request.
        // ---------------------------------------------------------------------
        logger.info('Incoming request', {
            method: req.method,
            path: req.path,
            correlation_id: correlationId,
            is_new_flow: isNewFlow,
            source_service: req.sourceService,
            query: Object.keys(req.query).length > 0 ? req.query : undefined,
        });

        // ---------------------------------------------------------------------
        // Step 5: Log the response when it completes
        // ---------------------------------------------------------------------
        // We hook into the 'finish' event to log the response status and
        // calculate request duration. This creates a complete request trace.
        // ---------------------------------------------------------------------
        const originalEnd = res.end;
        res.end = function (...args) {
            const duration = Date.now() - req.requestStartTime;

            logger.info('Request completed', {
                method: req.method,
                path: req.path,
                status: res.statusCode,
                duration_ms: duration,
                correlation_id: correlationId,
            });

            // Call the original end method
            originalEnd.apply(res, args);
        };

        // Continue to the next middleware/route handler
        next();
    };
}

// =============================================================================
// extractCorrelationId() — Extract correlation ID from various sources
// =============================================================================
// Utility function for extracting correlation ID from different contexts:
//   - HTTP headers (for API requests)
//   - Kafka message headers (for event consumption)
//   - Event objects (for replay)
//
// PARAMETERS:
//   @param {object} source — The source to extract from (req, message, event)
//
// RETURNS:
//   The correlation ID string, or null if not found
// =============================================================================
function extractCorrelationId(source) {
    // From Express request
    if (source.headers && source.headers[CORRELATION_HEADER]) {
        return source.headers[CORRELATION_HEADER];
    }

    // From Kafka message headers
    if (source.headers && source.headers['correlation-id']) {
        const header = source.headers['correlation-id'];
        return Buffer.isBuffer(header) ? header.toString() : header;
    }

    // From event object
    if (source.correlation_id) {
        return source.correlation_id;
    }

    // From correlationId property (camelCase variant)
    if (source.correlationId) {
        return source.correlationId;
    }

    return null;
}

module.exports = {
    correlationMiddleware,
    generateCorrelationId,
    extractCorrelationId,
    CORRELATION_HEADER,
    SOURCE_SERVICE_HEADER,
    REQUEST_START_HEADER,
};
