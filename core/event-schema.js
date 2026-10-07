// =============================================================================
// EVENT SCHEMA — The Foundation of the Entire System
// =============================================================================
// This module defines the CANONICAL event structure used across ALL services.
//
// WHY THIS MATTERS:
//   - Without a consistent schema, events become unstructured logs
//   - Schema validation prevents malformed events from entering Kafka
//   - Every field has a specific purpose for debugging and replay
//
// SCHEMA VERSION:
//   We version the schema so consumers can handle schema evolution gracefully.
//   If you add fields, bump the version and handle backward compatibility.
//
// EVENT STRUCTURE:
//   {
//     event_id:       "uuid-v4"           → Globally unique event identifier
//     event_type:     "ORDER_CREATED"     → What happened (ENTITY_ACTION format)
//     correlation_id: "req-123"           → Links all events in a request flow
//     timestamp:      1710000000000       → Unix epoch milliseconds
//     service:        "order-service"     → Which service produced this event
//     schema_version: "1.0"              → For backward compatibility
//     payload:        { ... }            → Domain-specific data
//     metadata:       { ... }            → System metadata (topic, partition, etc.)
//   }
//
// =============================================================================

const { v4: uuidv4 } = require('uuid');

// =============================================================================
// SCHEMA VERSION
// =============================================================================
// Bump this when you add/remove/change fields in the event structure.
// Consumers should check this version and handle unknown versions gracefully.
// =============================================================================
const EVENT_SCHEMA_VERSION = '1.0';

// =============================================================================
// EVENT TYPES — Centralized Enum of All Event Types
// =============================================================================
// Using a centralized enum prevents typos and enables IDE autocomplete.
// Naming convention: ENTITY_ACTION (e.g., ORDER_CREATED, PAYMENT_FAILED)
//
// Categories:
//   ORDER_*    → Order lifecycle events (produced by order-service)
//   PAYMENT_*  → Payment lifecycle events (produced by payment-service)
//   REPLAY_*   → System events (produced by replay-engine)
// =============================================================================
const EventTypes = {
    // -------------------------------------------------------------------------
    // ORDER EVENTS — Produced by order-service
    // -------------------------------------------------------------------------
    ORDER_CREATED:    'ORDER_CREATED',     // New order placed by customer
    ORDER_CONFIRMED:  'ORDER_CONFIRMED',   // Order validated and confirmed
    ORDER_UPDATED:    'ORDER_UPDATED',     // Order details modified
    ORDER_CANCELLED:  'ORDER_CANCELLED',   // Order cancelled by customer or system
    ORDER_COMPLETED:  'ORDER_COMPLETED',   // Order fulfilled and completed

    // -------------------------------------------------------------------------
    // PAYMENT EVENTS — Produced by payment-service
    // -------------------------------------------------------------------------
    PAYMENT_INITIATED: 'PAYMENT_INITIATED', // Payment process started
    PAYMENT_PROCESSING:'PAYMENT_PROCESSING',// Payment being processed by gateway
    PAYMENT_SUCCESS:   'PAYMENT_SUCCESS',   // Payment completed successfully
    PAYMENT_FAILED:    'PAYMENT_FAILED',    // Payment failed (insufficient funds, etc.)
    PAYMENT_REFUNDED:  'PAYMENT_REFUNDED',  // Payment refunded

    // -------------------------------------------------------------------------
    // SYSTEM / REPLAY EVENTS — Produced by replay-engine
    // -------------------------------------------------------------------------
    REPLAY_STARTED:   'REPLAY_STARTED',    // Replay session initiated
    REPLAY_COMPLETED: 'REPLAY_COMPLETED',  // Replay session finished
    REPLAY_FAILED:    'REPLAY_FAILED',     // Replay session encountered error
    REPLAY_EVENT_PROCESSED: 'REPLAY_EVENT_PROCESSED', // Individual event replayed
};

// =============================================================================
// TOPIC MAPPING — Which topic each event type belongs to
// =============================================================================
// This mapping ensures events are routed to the correct Kafka topic.
// CRITICAL: The topic determines the ordering guarantees for the event.
// =============================================================================
const EventTopicMap = {
    [EventTypes.ORDER_CREATED]:      'orders-events',
    [EventTypes.ORDER_CONFIRMED]:    'orders-events',
    [EventTypes.ORDER_UPDATED]:      'orders-events',
    [EventTypes.ORDER_CANCELLED]:    'orders-events',
    [EventTypes.ORDER_COMPLETED]:    'orders-events',
    [EventTypes.PAYMENT_INITIATED]:  'payments-events',
    [EventTypes.PAYMENT_PROCESSING]: 'payments-events',
    [EventTypes.PAYMENT_SUCCESS]:    'payments-events',
    [EventTypes.PAYMENT_FAILED]:     'payments-events',
    [EventTypes.PAYMENT_REFUNDED]:   'payments-events',
    [EventTypes.REPLAY_STARTED]:     'system-events',
    [EventTypes.REPLAY_COMPLETED]:   'system-events',
    [EventTypes.REPLAY_FAILED]:      'system-events',
    [EventTypes.REPLAY_EVENT_PROCESSED]: 'system-events',
};

// =============================================================================
// createEvent() — Factory function for creating structured events
// =============================================================================
// Creates a new event with all required fields populated.
//
// PARAMETERS:
//   @param {string} eventType      — Must be a value from EventTypes enum
//   @param {string} correlationId  — Request flow identifier
//   @param {string} service        — Name of the producing service
//   @param {object} payload        — Domain-specific event data
//   @param {object} metadata       — Optional system metadata
//
// RETURNS:
//   A fully-formed event object ready for Kafka production
//
// EXAMPLE:
//   const event = createEvent(
//     EventTypes.ORDER_CREATED,
//     'req-123',
//     'order-service',
//     { orderId: 'ord-001', customerId: 'cust-001', totalAmount: 1998 }
//   );
// =============================================================================
function createEvent(eventType, correlationId, service, payload = {}, metadata = {}) {
    // Validate event type against known types
    if (!Object.values(EventTypes).includes(eventType)) {
        throw new Error(
            `Unknown event type: "${eventType}". ` +
            `Valid types: ${Object.values(EventTypes).join(', ')}`
        );
    }

    // Validate required fields
    if (!correlationId) {
        throw new Error('correlation_id is required — cannot create an event without request tracing');
    }
    if (!service) {
        throw new Error('service name is required — cannot create an untraceable event');
    }

    return {
        // Unique event identifier — UUID v4 ensures global uniqueness
        // without any coordination between services
        event_id: uuidv4(),

        // What happened — uses ENTITY_ACTION naming convention
        event_type: eventType,

        // Request flow identifier — THE MOST IMPORTANT FIELD
        // This links together all events that belong to the same
        // distributed transaction
        correlation_id: correlationId,

        // Unix timestamp in milliseconds — used for ordering during replay
        // Using Date.now() provides millisecond precision
        // IMPORTANT: This is the event CREATION time, not the ingestion time
        timestamp: Date.now(),

        // Which service produced this event — used for timeline visualization
        service: service,

        // Schema version — for backward compatibility during schema evolution
        schema_version: EVENT_SCHEMA_VERSION,

        // Domain-specific data — varies by event type
        // Examples:
        //   ORDER_CREATED:     { orderId, customerId, items, totalAmount }
        //   PAYMENT_SUCCESS:   { paymentId, orderId, transactionRef }
        //   PAYMENT_FAILED:    { paymentId, orderId, error, errorCode }
        payload: payload,

        // System metadata — not part of the business domain
        // Populated by the producer and enriched by the ingestor
        // Examples: source_topic, partition, offset, producer_ip
        metadata: {
            source: service,
            ...metadata,
        },
    };
}

// =============================================================================
// validateEvent() — Validates an event object against the schema
// =============================================================================
// Used by consumers to verify incoming events before processing.
// Returns { valid: true } or { valid: false, errors: [...] }
//
// WHY VALIDATE ON THE CONSUMER SIDE?
//   - Kafka is schema-agnostic — it doesn't validate message contents
//   - A malformed event could crash the consumer or corrupt data
//   - Validation at both producer AND consumer sides ensures robustness
//   - In production, you'd use Confluent Schema Registry for this
// =============================================================================
function validateEvent(event) {
    const errors = [];

    // Check required fields exist
    if (!event.event_id)       errors.push('Missing required field: event_id');
    if (!event.event_type)     errors.push('Missing required field: event_type');
    if (!event.correlation_id) errors.push('Missing required field: correlation_id');
    if (!event.timestamp)      errors.push('Missing required field: timestamp');
    if (!event.service)        errors.push('Missing required field: service');

    // Validate event_type is known
    if (event.event_type && !Object.values(EventTypes).includes(event.event_type)) {
        errors.push(`Unknown event_type: "${event.event_type}"`);
    }

    // Validate timestamp is a reasonable value (not in the far future or past)
    if (event.timestamp) {
        const now = Date.now();
        const oneHourMs = 3600000;
        const oneYearMs = 365 * 24 * 3600000;

        if (event.timestamp > now + oneHourMs) {
            errors.push(`Timestamp is in the future: ${event.timestamp}`);
        }
        if (event.timestamp < now - oneYearMs) {
            errors.push(`Timestamp is more than 1 year old: ${event.timestamp}`);
        }
    }

    // Validate payload is an object
    if (event.payload && typeof event.payload !== 'object') {
        errors.push('Payload must be an object');
    }

    return {
        valid: errors.length === 0,
        errors,
    };
}

// =============================================================================
// getTopicForEvent() — Returns the Kafka topic for a given event type
// =============================================================================
function getTopicForEvent(eventType) {
    const topic = EventTopicMap[eventType];
    if (!topic) {
        throw new Error(`No topic mapping found for event type: ${eventType}`);
    }
    return topic;
}

module.exports = {
    createEvent,
    validateEvent,
    getTopicForEvent,
    EventTypes,
    EventTopicMap,
    EVENT_SCHEMA_VERSION,
};
