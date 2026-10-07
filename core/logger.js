// =============================================================================
// STRUCTURED LOGGER — Centralized Logging with Correlation Context
// =============================================================================
// This module provides structured JSON logging for all services.
//
// WHY STRUCTURED LOGGING?
//   - Plain text logs (console.log) are nearly useless in distributed systems
//   - Structured logs (JSON format) can be:
//     → Aggregated by tools like ELK, Datadog, Splunk
//     → Filtered by correlation_id, service, log level
//     → Searched with queries like: service="order-service" AND level="error"
//     → Visualized in dashboards
//
// FEATURES:
//   1. JSON format by default (machine-parseable)
//   2. Colorized console output for development (human-readable)
//   3. Automatic service name attachment
//   4. Timestamp in ISO 8601 format
//   5. Log level filtering
//
// LOG LEVELS (in order of severity):
//   error → Something broke, needs immediate attention
//   warn  → Something unexpected but handled, might need investigation
//   info  → Normal operational events (request received, event emitted)
//   debug → Detailed information for active debugging
//
// USAGE:
//   const { createLogger } = require('@chronoscope/core');
//   const logger = createLogger('order-service');
//   logger.info('Order created', { orderId: 'ord-001', correlation_id: 'req-123' });
//
// OUTPUT:
//   {"level":"info","message":"Order created","service":"order-service",
//    "orderId":"ord-001","correlation_id":"req-123","timestamp":"2024-03-10T12:00:00.000Z"}
// =============================================================================

const winston = require('winston');

// =============================================================================
// LOG FORMAT CONFIGURATION
// =============================================================================
// We define two formats:
//   1. JSON format → for production (machine-parseable, sent to log aggregator)
//   2. Console format → for development (colorized, human-readable)
//
// The format is selected based on NODE_ENV environment variable.
// =============================================================================

// Custom format that adds the service name to every log entry
const serviceFormat = (serviceName) => {
    return winston.format((info) => {
        info.service = serviceName;
        return info;
    })();
};

// =============================================================================
// createLogger() — Factory function for creating a service-specific logger
// =============================================================================
// Creates a Winston logger instance configured for a specific service.
//
// PARAMETERS:
//   @param {string} serviceName — Name of the service using this logger
//                                 (attached to every log entry)
//   @param {object} options     — Optional configuration
//     - level: string           → Minimum log level (default: 'info')
//     - silent: boolean         → Suppress all output (default: false)
//
// RETURNS:
//   A Winston logger instance with info(), error(), warn(), debug() methods
//
// EXAMPLE:
//   const logger = createLogger('order-service');
//   logger.info('Processing order', { orderId: 'ord-001' });
//   logger.error('Payment failed', { error: 'Insufficient funds' });
// =============================================================================
function createLogger(serviceName, options = {}) {
    const { level = 'info', silent = false } = options;
    const isProduction = process.env.NODE_ENV === 'production';

    // -------------------------------------------------------------------------
    // Define log transports (where logs are sent)
    // -------------------------------------------------------------------------
    // Development: Colorized console output for easy reading
    // Production: JSON format to stdout (for log aggregation pipelines)
    //
    // In production, you'd typically add:
    //   - File transport (for local persistence)
    //   - HTTP transport (for sending to Datadog/ELK)
    //   - Kafka transport (for log event streaming)
    // -------------------------------------------------------------------------
    const transports = [];

    if (isProduction) {
        // Production: JSON to stdout (for container log drivers)
        transports.push(
            new winston.transports.Console({
                format: winston.format.combine(
                    serviceFormat(serviceName),
                    winston.format.timestamp({ format: 'YYYY-MM-DDTHH:mm:ss.SSSZ' }),
                    winston.format.errors({ stack: true }),
                    winston.format.json()
                ),
            })
        );
    } else {
        // Development: Colorized, human-readable console output
        transports.push(
            new winston.transports.Console({
                format: winston.format.combine(
                    serviceFormat(serviceName),
                    winston.format.timestamp({ format: 'HH:mm:ss.SSS' }),
                    winston.format.errors({ stack: true }),
                    winston.format.colorize(),
                    winston.format.printf(({ timestamp, level, message, service, ...meta }) => {
                        // Build the log line with service context
                        const serviceTag = `[${service}]`.padEnd(20);
                        const metaStr = Object.keys(meta).length > 0
                            ? ` ${JSON.stringify(meta)}`
                            : '';
                        return `${timestamp} ${level} ${serviceTag} ${message}${metaStr}`;
                    })
                ),
            })
        );
    }

    // -------------------------------------------------------------------------
    // Create and return the Winston logger instance
    // -------------------------------------------------------------------------
    const logger = winston.createLogger({
        level: process.env.LOG_LEVEL || level,
        silent,
        transports,
        // Don't exit on uncaught exceptions — let the process manager handle it
        exitOnError: false,
    });

    return logger;
}

module.exports = { createLogger };
