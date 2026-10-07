// =============================================================================
// ORDER ROUTES — REST API Endpoints for Order Management
// =============================================================================
// This module defines the HTTP routes for the Order Service.
// Routes are thin — they handle:
//   1. Request parsing and validation
//   2. Delegating to the order handler (business logic)
//   3. Formatting and sending responses
//
// ENDPOINTS:
//   POST   /orders           → Create a new order
//   GET    /orders           → List all orders (with pagination)
//   GET    /orders/:id       → Get a specific order by ID
//   PUT    /orders/:id       → Update an existing order
//   DELETE /orders/:id       → Cancel an order
//
// DESIGN PRINCIPLE: "Thin routes, fat handlers"
//   - Routes should NOT contain business logic
//   - Routes should NOT directly interact with Kafka or the database
//   - All business logic lives in the handler layer
//   - This makes the handler testable without HTTP overhead
//
// CORRELATION ID:
//   Every request has req.correlationId (set by correlation middleware).
//   This is passed to the handler and included in all emitted events.
//   The response always includes the x-correlation-id header.
// =============================================================================

const express = require('express');

// =============================================================================
// createOrderRoutes() — Factory function for order API routes
// =============================================================================
// PARAMETERS:
//   @param {object} orderHandler — The order handler with business logic methods
//
// RETURNS:
//   Express Router with all order endpoints configured
// =============================================================================
function createOrderRoutes(orderHandler) {
    const router = express.Router();

    // =========================================================================
    // POST /orders — Create a New Order
    // =========================================================================
    // Creates a new order in the database and emits an ORDER_CREATED event
    // to Kafka. The event triggers downstream processing (payment, inventory, etc.)
    //
    // REQUEST BODY:
    //   {
    //     "customerId": "cust-001",
    //     "items": [
    //       { "name": "Widget Pro", "quantity": 2, "price": 999 }
    //     ]
    //   }
    //
    // RESPONSE (201 Created):
    //   {
    //     "success": true,
    //     "order": { orderId, customerId, items, totalAmount, status, correlationId },
    //     "event": { event_id, event_type, correlation_id, timestamp },
    //     "correlationId": "req-..."
    //   }
    //
    // FLOW:
    //   1. Validate request body (customerId and items required)
    //   2. Calculate total amount from items
    //   3. Save order to PostgreSQL
    //   4. Emit ORDER_CREATED event to Kafka
    //   5. Return order and event details
    //
    // WHY EMIT AFTER DB WRITE?
    //   - If we emit first and DB write fails, we have a phantom event
    //   - If we write first and emit fails, we retry the emit
    //   - This gives us "at-least-once" event emission
    //   - The Outbox Pattern is a more robust alternative for production
    // =========================================================================
    router.post('/', async (req, res) => {
        try {
            // -----------------------------------------------------------------
            // Input Validation
            // -----------------------------------------------------------------
            // Validate required fields before processing.
            // In production, use a validation library (Joi, Zod, etc.)
            // -----------------------------------------------------------------
            const { customerId, items } = req.body;

            if (!customerId) {
                return res.status(400).json({
                    success: false,
                    error: 'customerId is required',
                    correlationId: req.correlationId,
                });
            }

            if (!items || !Array.isArray(items) || items.length === 0) {
                return res.status(400).json({
                    success: false,
                    error: 'items must be a non-empty array',
                    correlationId: req.correlationId,
                });
            }

            // Validate each item has required fields
            for (const item of items) {
                if (!item.name || !item.quantity || !item.price) {
                    return res.status(400).json({
                        success: false,
                        error: 'Each item must have name, quantity, and price',
                        correlationId: req.correlationId,
                    });
                }
            }

            // -----------------------------------------------------------------
            // Delegate to handler (business logic)
            // -----------------------------------------------------------------
            const result = await orderHandler.createOrder({
                customerId,
                items,
                correlationId: req.correlationId,
            });

            // -----------------------------------------------------------------
            // Return response with correlation ID
            // -----------------------------------------------------------------
            res.status(201).json({
                success: true,
                ...result,
                correlationId: req.correlationId,
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message,
                correlationId: req.correlationId,
            });
        }
    });

    // =========================================================================
    // GET /orders — List All Orders (with Pagination)
    // =========================================================================
    // Returns a paginated list of orders.
    //
    // QUERY PARAMETERS:
    //   - page: Page number (default: 1)
    //   - limit: Items per page (default: 20, max: 100)
    //   - status: Filter by status (optional)
    //   - customerId: Filter by customer (optional)
    //
    // RESPONSE:
    //   {
    //     "success": true,
    //     "orders": [...],
    //     "pagination": { page, limit, total }
    //   }
    // =========================================================================
    router.get('/', async (req, res) => {
        try {
            const {
                page = 1,
                limit = 20,
                status,
                customerId,
            } = req.query;

            const result = await orderHandler.listOrders({
                page: parseInt(page),
                limit: Math.min(parseInt(limit), 100),
                status,
                customerId,
            });

            res.json({
                success: true,
                ...result,
                correlationId: req.correlationId,
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message,
                correlationId: req.correlationId,
            });
        }
    });

    // =========================================================================
    // GET /orders/:id — Get a Specific Order
    // =========================================================================
    // Returns a single order by its ID.
    //
    // RESPONSE (200 OK):
    //   { "success": true, "order": {...} }
    //
    // RESPONSE (404 Not Found):
    //   { "success": false, "error": "Order not found" }
    // =========================================================================
    router.get('/:id', async (req, res) => {
        try {
            const order = await orderHandler.getOrder(req.params.id);

            if (!order) {
                return res.status(404).json({
                    success: false,
                    error: 'Order not found',
                    correlationId: req.correlationId,
                });
            }

            res.json({
                success: true,
                order,
                correlationId: req.correlationId,
            });

        } catch (error) {
            res.status(500).json({
                success: false,
                error: error.message,
                correlationId: req.correlationId,
            });
        }
    });

    // =========================================================================
    // PUT /orders/:id — Update an Existing Order
    // =========================================================================
    // Updates order details and emits an ORDER_UPDATED event.
    //
    // REQUEST BODY:
    //   { "items": [...], "status": "CONFIRMED" }
    //
    // BUSINESS RULES:
    //   - Cannot update a cancelled order
    //   - Cannot update a completed order
    //   - Status transitions must be valid
    //
    // RESPONSE (200 OK):
    //   { "success": true, "order": {...}, "event": {...} }
    // =========================================================================
    router.put('/:id', async (req, res) => {
        try {
            const result = await orderHandler.updateOrder(req.params.id, {
                ...req.body,
                correlationId: req.correlationId,
            });

            if (!result) {
                return res.status(404).json({
                    success: false,
                    error: 'Order not found',
                    correlationId: req.correlationId,
                });
            }

            res.json({
                success: true,
                ...result,
                correlationId: req.correlationId,
            });

        } catch (error) {
            res.status(error.statusCode || 500).json({
                success: false,
                error: error.message,
                correlationId: req.correlationId,
            });
        }
    });

    // =========================================================================
    // DELETE /orders/:id — Cancel an Order
    // =========================================================================
    // Cancels an order and emits an ORDER_CANCELLED event.
    //
    // BUSINESS RULES:
    //   - Cannot cancel an already cancelled order
    //   - Cannot cancel a completed order
    //   - Cancellation triggers payment refund (via event)
    //
    // RESPONSE (200 OK):
    //   { "success": true, "order": {...}, "event": {...} }
    // =========================================================================
    router.delete('/:id', async (req, res) => {
        try {
            const result = await orderHandler.cancelOrder(req.params.id, {
                reason: req.body.reason || 'Customer requested cancellation',
                correlationId: req.correlationId,
            });

            if (!result) {
                return res.status(404).json({
                    success: false,
                    error: 'Order not found',
                    correlationId: req.correlationId,
                });
            }

            res.json({
                success: true,
                ...result,
                correlationId: req.correlationId,
            });

        } catch (error) {
            res.status(error.statusCode || 500).json({
                success: false,
                error: error.message,
                correlationId: req.correlationId,
            });
        }
    });

    return router;
}

module.exports = { createOrderRoutes };
