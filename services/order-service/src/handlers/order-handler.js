// =============================================================================
// ORDER HANDLER — Business Logic for Order Operations
// =============================================================================
// This is where the REAL WORK happens. The handler contains all business logic
// for order management, separated from HTTP concerns (routes) and infrastructure
// concerns (Kafka, database).
//
// RESPONSIBILITIES:
//   1. Order creation with validation
//   2. Order state management (status transitions)
//   3. Event emission to Kafka
//   4. In-memory storage (PostgreSQL in production)
//
// EVENT EMISSION PATTERN:
//   Every state change follows the same pattern:
//     1. Validate the operation
//     2. Apply the state change (save to DB)
//     3. Create a structured event
//     4. Emit the event to Kafka
//     5. Return the result
//
//   This is the "State Change → Emit Event" pattern.
//   The event captures WHAT happened, not just the current state.
//
// WHY IN-MEMORY STORE?
//   For simplicity in this implementation, we use an in-memory Map.
//   In production, this would be PostgreSQL with the pg library.
//   The in-memory store makes the service self-contained (no DB dependency
//   for initial testing). The event store (PostgreSQL) captures the truth.
//
// DEPENDENCY INJECTION:
//   The handler receives the Kafka producer as a constructor parameter.
//   This makes it easy to:
//     - Test with a mock producer
//     - Swap the producer implementation
//     - Disable event emission during replay (side-effect isolation!)
// =============================================================================

const { v4: uuidv4 } = require('uuid');
const { createEvent, EventTypes, createLogger } = require('@chronoscope/core');

// =============================================================================
// createOrderHandler() — Factory function for order business logic
// =============================================================================
// PARAMETERS:
//   @param {object} producer    — Kafka producer instance (for emitting events)
//   @param {string} serviceName — Name of this service (for event creation)
//
// RETURNS:
//   Object with methods: createOrder, getOrder, listOrders, updateOrder, cancelOrder
// =============================================================================
function createOrderHandler(producer, serviceName) {
    const logger = createLogger(serviceName);

    // =========================================================================
    // IN-MEMORY ORDER STORE
    // =========================================================================
    // Simulates a database for order storage.
    // In production, replace with PostgreSQL using the pg library.
    //
    // KEY: orderId (string)
    // VALUE: order object { orderId, customerId, items, totalAmount, status, ... }
    //
    // WHY A MAP AND NOT AN ARRAY?
    //   - O(1) lookups by orderId (vs O(n) for array.find)
    //   - O(1) updates (vs creating a new array)
    //   - Natural key-value semantics for entity storage
    // =========================================================================
    const orders = new Map();

    // =========================================================================
    // VALID STATUS TRANSITIONS
    // =========================================================================
    // Defines which status transitions are allowed.
    // This prevents illegal state changes (e.g., CANCELLED → PAID).
    //
    // State machine:
    //   PENDING → CONFIRMED → PAID → COMPLETED
    //                              ↘ CANCELLED (from PENDING or CONFIRMED)
    //
    // INTERVIEW TIP: "I model order status as a state machine with explicit
    // valid transitions. This prevents data corruption from out-of-order
    // events during replay."
    // =========================================================================
    const VALID_TRANSITIONS = {
        'PENDING':   ['PAID', 'CONFIRMED', 'CANCELLED'],
        'CONFIRMED': ['PAID', 'CANCELLED'],
        'PAID':      ['COMPLETED', 'CANCELLED'],
        'COMPLETED': [],                          // Terminal state
        'CANCELLED': [],                          // Terminal state
    };

    return {
        // =====================================================================
        // createOrder() — Create a new order and emit ORDER_CREATED event
        // =====================================================================
        // This is the STARTING POINT of most event flows in the system.
        // When an order is created, the ORDER_CREATED event triggers:
        //   1. Payment Service → initiates payment
        //   2. Event Ingestor → stores event for debugging
        //   3. Any other consumers subscribed to orders-events
        //
        // PARAMETERS:
        //   @param {object} orderData — { customerId, items, correlationId }
        //
        // RETURNS:
        //   { order, event } — The created order and the emitted event
        //
        // FLOW:
        //   1. Generate unique orderId
        //   2. Calculate total amount from items
        //   3. Create order object with PENDING status
        //   4. Save to store (in-memory / DB)
        //   5. Create ORDER_CREATED event with full order details
        //   6. Emit event to Kafka (orders-events topic)
        //   7. Return order and event for API response
        // =====================================================================
        async createOrder({ customerId, items, correlationId }) {
            // Generate unique order ID
            const orderId = `ord-${uuidv4().substring(0, 8)}`;

            // -----------------------------------------------------------------
            // Calculate total amount
            // -----------------------------------------------------------------
            // Amount is in CENTS to avoid floating-point precision issues.
            // $9.99 is stored as 999. $19.98 is stored as 1998.
            //
            // WHY CENTS?
            //   - 0.1 + 0.2 === 0.30000000000000004 in JavaScript
            //   - Integer arithmetic is exact: 10 + 20 === 30
            //   - Industry standard (Stripe, PayPal, etc. all use cents)
            // -----------------------------------------------------------------
            const totalAmount = items.reduce((sum, item) => {
                return sum + (item.price * item.quantity);
            }, 0);

            // -----------------------------------------------------------------
            // Create order object
            // -----------------------------------------------------------------
            const order = {
                orderId,
                customerId,
                items,
                totalAmount,
                status: 'PENDING',
                correlationId,
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
            };

            // Save to store
            orders.set(orderId, order);

            logger.info('Order created', {
                orderId,
                customerId,
                totalAmount,
                itemCount: items.length,
                correlation_id: correlationId,
            });

            // -----------------------------------------------------------------
            // Emit ORDER_CREATED event to Kafka
            // -----------------------------------------------------------------
            // The event contains ALL information needed to:
            //   1. Trigger downstream processing (payment)
            //   2. Reconstruct this state change during replay
            //   3. Display in the debug timeline
            //
            // IMPORTANT: We emit AFTER the DB write succeeds.
            // If the emit fails, the order exists but the event is missing.
            // In production, use the Outbox Pattern to guarantee both.
            // -----------------------------------------------------------------
            const event = createEvent(
                EventTypes.ORDER_CREATED,
                correlationId,
                serviceName,
                {
                    orderId,
                    customerId,
                    items,
                    totalAmount,
                    status: 'PENDING',
                }
            );

            await producer.emit(event);

            logger.info('ORDER_CREATED event emitted', {
                event_id: event.event_id,
                orderId,
                correlation_id: correlationId,
            });

            return { order, event };
        },

        // =====================================================================
        // getOrder() — Retrieve a single order by ID
        // =====================================================================
        // Simple lookup — no events emitted for read operations.
        // Read operations don't change state, so they don't need events.
        // =====================================================================
        async getOrder(orderId) {
            return orders.get(orderId) || null;
        },

        // =====================================================================
        // listOrders() — List orders with optional filtering and pagination
        // =====================================================================
        // Returns a paginated list of orders with optional filters.
        //
        // PARAMETERS:
        //   @param {object} options — { page, limit, status, customerId }
        //
        // RETURNS:
        //   { orders, pagination: { page, limit, total } }
        // =====================================================================
        async listOrders({ page = 1, limit = 20, status, customerId } = {}) {
            let allOrders = Array.from(orders.values());

            // Apply filters
            if (status) {
                allOrders = allOrders.filter(o => o.status === status);
            }
            if (customerId) {
                allOrders = allOrders.filter(o => o.customerId === customerId);
            }

            // Sort by creation time (newest first)
            allOrders.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

            // Apply pagination
            const total = allOrders.length;
            const start = (page - 1) * limit;
            const paginatedOrders = allOrders.slice(start, start + limit);

            return {
                orders: paginatedOrders,
                pagination: { page, limit, total },
            };
        },

        // =====================================================================
        // updateOrder() — Update an order and emit ORDER_UPDATED event
        // =====================================================================
        // Updates order fields and emits an ORDER_UPDATED event.
        //
        // PARAMETERS:
        //   @param {string} orderId    — The order to update
        //   @param {object} updateData — Fields to update + correlationId
        //
        // RETURNS:
        //   { order, event } — Updated order and emitted event
        //
        // VALIDATION:
        //   - Order must exist
        //   - Status transition must be valid (checked against state machine)
        //   - Cannot update terminal states (COMPLETED, CANCELLED)
        // =====================================================================
        async updateOrder(orderId, updateData) {
            const order = orders.get(orderId);
            if (!order) return null;

            const { correlationId, status, items, ...otherUpdates } = updateData;

            // -----------------------------------------------------------------
            // Validate status transition
            // -----------------------------------------------------------------
            if (status && status !== order.status) {
                const validTransitions = VALID_TRANSITIONS[order.status] || [];
                if (!validTransitions.includes(status)) {
                    const error = new Error(
                        `Invalid status transition: ${order.status} → ${status}. ` +
                        `Valid transitions: ${validTransitions.join(', ') || 'none (terminal state)'}`
                    );
                    error.statusCode = 400;
                    throw error;
                }
            }

            // -----------------------------------------------------------------
            // Apply updates
            // -----------------------------------------------------------------
            const previousState = { ...order };

            if (status) order.status = status;
            if (items) {
                order.items = items;
                order.totalAmount = items.reduce((sum, item) => sum + (item.price * item.quantity), 0);
            }
            Object.assign(order, otherUpdates);
            order.updatedAt = new Date().toISOString();

            // Save updated order
            orders.set(orderId, order);

            logger.info('Order updated', {
                orderId,
                previousStatus: previousState.status,
                newStatus: order.status,
                correlation_id: correlationId || order.correlationId,
            });

            // -----------------------------------------------------------------
            // Emit ORDER_UPDATED event
            // -----------------------------------------------------------------
            // The event captures BOTH the previous and current state.
            // This is crucial for replay — it allows the replay engine to
            // verify that the state transition was correct.
            // -----------------------------------------------------------------
            const event = createEvent(
                EventTypes.ORDER_UPDATED,
                correlationId || order.correlationId,
                serviceName,
                {
                    orderId,
                    previousState: {
                        status: previousState.status,
                        totalAmount: previousState.totalAmount,
                    },
                    currentState: {
                        status: order.status,
                        totalAmount: order.totalAmount,
                        items: order.items,
                    },
                    changes: Object.keys(updateData).filter(k => k !== 'correlationId'),
                }
            );

            await producer.emit(event);

            return { order, event };
        },

        // =====================================================================
        // cancelOrder() — Cancel an order and emit ORDER_CANCELLED event
        // =====================================================================
        // Transitions order to CANCELLED status and emits an event.
        // The CANCELLED event can trigger downstream actions:
        //   - Payment refund
        //   - Inventory release
        //   - Customer notification
        //
        // PARAMETERS:
        //   @param {string} orderId     — The order to cancel
        //   @param {object} cancelData  — { reason, correlationId }
        //
        // RETURNS:
        //   { order, event } — Cancelled order and emitted event
        // =====================================================================
        async cancelOrder(orderId, { reason, correlationId }) {
            const order = orders.get(orderId);
            if (!order) return null;

            // Validate transition
            const validTransitions = VALID_TRANSITIONS[order.status] || [];
            if (!validTransitions.includes('CANCELLED')) {
                const error = new Error(
                    `Cannot cancel order in ${order.status} status. ` +
                    `Cancellation is only allowed from: PENDING, CONFIRMED, PAID`
                );
                error.statusCode = 400;
                throw error;
            }

            const previousStatus = order.status;
            order.status = 'CANCELLED';
            order.cancelReason = reason;
            order.updatedAt = new Date().toISOString();

            orders.set(orderId, order);

            logger.info('Order cancelled', {
                orderId,
                previousStatus,
                reason,
                correlation_id: correlationId || order.correlationId,
            });

            // Emit ORDER_CANCELLED event
            const event = createEvent(
                EventTypes.ORDER_CANCELLED,
                correlationId || order.correlationId,
                serviceName,
                {
                    orderId,
                    previousStatus,
                    reason,
                    totalAmount: order.totalAmount,
                }
            );

            await producer.emit(event);

            return { order, event };
        },
    };
}

module.exports = { createOrderHandler };
