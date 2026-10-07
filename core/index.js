// =============================================================================
// CORE MODULE — Public API
// =============================================================================
// This is the entry point for the @chronoscope/core package.
// All shared utilities are exported from here for use by microservices.
//
// USAGE:
//   const { createEvent, EventTypes, createProducer, correlationMiddleware } = require('@chronoscope/core');
// =============================================================================

// Load environment from the repo-root `.env` if present. Resolved by absolute
// path (relative to this file) so it works no matter which service's cwd we run
// from — including `npm start --workspace=...`, which runs from the service dir.
// Missing file is a no-op; real env vars (e.g. from Docker Compose) always win.
require('dotenv').config({
    path: require('path').resolve(__dirname, '../.env'),
});

const { createEvent, validateEvent, EventTypes, EVENT_SCHEMA_VERSION } = require('./event-schema');
const { createProducer } = require('./kafka-producer');
const { createConsumer } = require('./kafka-consumer');
const { correlationMiddleware, generateCorrelationId } = require('./correlation-middleware');
const { createLogger } = require('./logger');

module.exports = {
    // Event Schema
    createEvent,
    validateEvent,
    EventTypes,
    EVENT_SCHEMA_VERSION,

    // Kafka Utilities
    createProducer,
    createConsumer,

    // Middleware
    correlationMiddleware,
    generateCorrelationId,

    // Logging
    createLogger,
};
