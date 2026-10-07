// =============================================================================
// TIMELINE ROUTES — Formatted Timeline Data for Debug UI
// =============================================================================
// These routes provide pre-formatted timeline data optimized for the
// React Debug UI's visualization components.
//
// WHAT MAKES THIS DIFFERENT FROM /events/correlation/:id?
//   The events endpoint returns raw event data.
//   The timeline endpoint returns FORMATTED data with:
//     - Time deltas (how long between events)
//     - Service swim lanes (group events by service)
//     - Failure highlights (which events failed)
//     - State transitions (what changed at each step)
//     - Duration metrics (total flow time, service times)
//
// This pre-processing happens server-side to keep the UI fast and simple.
// =============================================================================

const express = require('express');

function createTimelineRoutes(pool) {
    const router = express.Router();

    // =========================================================================
    // GET /timeline/:correlationId — Full Formatted Timeline
    // =========================================================================
    // Returns a visualization-ready timeline for a correlation_id.
    //
    // RESPONSE STRUCTURE:
    //   {
    //     correlationId: "req-123",
    //     timeline: [
    //       {
    //         event_id, event_type, service, timestamp,
    //         time_delta_ms,        // Time since previous event
    //         time_from_start_ms,   // Time since first event
    //         is_failure,           // Boolean — highlight in red
    //         swim_lane,            // Service name for swim lane positioning
    //         payload               // Full event data
    //       }
    //     ],
    //     summary: {
    //       totalEvents, duration_ms, services,
    //       failureCount, successCount
    //     },
    //     swimLanes: {
    //       "order-service": [...events],
    //       "payment-service": [...events]
    //     }
    //   }
    // =========================================================================
    router.get('/:correlationId', async (req, res) => {
        try {
            const correlationId = req.params.correlationId;

            // Fetch events ordered by timestamp
            const result = await pool.query(`
                SELECT event_id, correlation_id, event_type, service, timestamp, 
                       payload, metadata, created_at
                FROM events
                WHERE correlation_id = $1
                ORDER BY timestamp ASC
            `, [correlationId]);

            const events = result.rows;

            if (events.length === 0) {
                return res.status(404).json({
                    success: false,
                    error: `No events found for correlation_id: ${correlationId}`,
                });
            }

            // -----------------------------------------------------------------
            // Format timeline entries
            // -----------------------------------------------------------------
            // Add computed fields for the UI:
            //   - time_delta_ms: Time since the previous event
            //   - time_from_start_ms: Time since the first event
            //   - is_failure: Whether this event represents a failure
            //   - swim_lane: Service name for swim lane positioning
            //   - step_number: Sequential step number in the flow
            // -----------------------------------------------------------------
            const firstTimestamp = events[0].timestamp;
            const timeline = events.map((event, index) => {
                const previousTimestamp = index > 0 ? events[index - 1].timestamp : event.timestamp;

                return {
                    // Original event data
                    event_id: event.event_id,
                    event_type: event.event_type,
                    service: event.service,
                    timestamp: event.timestamp,
                    payload: event.payload,

                    // Computed fields for visualization
                    step_number: index + 1,
                    time_delta_ms: event.timestamp - previousTimestamp,
                    time_from_start_ms: event.timestamp - firstTimestamp,
                    is_failure: event.event_type.includes('FAILED') || event.event_type.includes('ERROR'),
                    is_success: event.event_type.includes('SUCCESS') || event.event_type.includes('COMPLETED'),
                    swim_lane: event.service,

                    // Human-readable descriptions
                    description: getEventDescription(event),
                    time_label: formatDuration(event.timestamp - firstTimestamp),
                };
            });

            // -----------------------------------------------------------------
            // Build swim lanes (group events by service)
            // -----------------------------------------------------------------
            // Swim lanes allow the UI to show events in parallel tracks,
            // one per service, making the distributed flow visible.
            // -----------------------------------------------------------------
            const swimLanes = {};
            for (const entry of timeline) {
                if (!swimLanes[entry.swim_lane]) {
                    swimLanes[entry.swim_lane] = [];
                }
                swimLanes[entry.swim_lane].push(entry);
            }

            // -----------------------------------------------------------------
            // Build summary
            // -----------------------------------------------------------------
            const services = [...new Set(events.map(e => e.service))];
            const failureCount = timeline.filter(e => e.is_failure).length;
            const lastEvent = events[events.length - 1];

            const summary = {
                correlationId,
                totalEvents: events.length,
                services,
                serviceCount: services.length,
                duration_ms: lastEvent.timestamp - firstTimestamp,
                duration_label: formatDuration(lastEvent.timestamp - firstTimestamp),
                firstEvent: {
                    type: events[0].event_type,
                    service: events[0].service,
                    timestamp: events[0].timestamp,
                },
                lastEvent: {
                    type: lastEvent.event_type,
                    service: lastEvent.service,
                    timestamp: lastEvent.timestamp,
                },
                failureCount,
                successCount: timeline.filter(e => e.is_success).length,
                hasFailures: failureCount > 0,
                status: failureCount > 0 ? 'FAILED' : 'SUCCESS',
            };

            res.json({
                success: true,
                correlationId,
                timeline,
                summary,
                swimLanes,
            });

        } catch (error) {
            res.status(500).json({ success: false, error: error.message });
        }
    });

    return router;
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Generate a human-readable description for an event.
 * Used in the timeline UI for quick scanning.
 */
function getEventDescription(event) {
    const payload = typeof event.payload === 'string'
        ? JSON.parse(event.payload)
        : event.payload;

    const descriptions = {
        'ORDER_CREATED': `New order ${payload.orderId || 'unknown'} created for customer ${payload.customerId || 'unknown'} — Amount: ${formatCents(payload.totalAmount)}`,
        'ORDER_UPDATED': `Order ${payload.orderId || 'unknown'} updated: ${payload.previousState?.status || '?'} → ${payload.currentState?.status || '?'}`,
        'ORDER_CANCELLED': `Order ${payload.orderId || 'unknown'} cancelled — Reason: ${payload.reason || 'unknown'}`,
        'ORDER_COMPLETED': `Order ${payload.orderId || 'unknown'} completed`,
        'PAYMENT_INITIATED': `Payment ${payload.paymentId || 'unknown'} initiated for order ${payload.orderId || 'unknown'} — Amount: ${formatCents(payload.amount)}`,
        'PAYMENT_SUCCESS': `Payment ${payload.paymentId || 'unknown'} successful — Ref: ${payload.transactionRef || 'N/A'}`,
        'PAYMENT_FAILED': `❌ Payment ${payload.paymentId || 'unknown'} FAILED — ${payload.error || 'Unknown error'}`,
        'PAYMENT_REFUNDED': `Payment ${payload.paymentId || 'unknown'} refunded — Amount: ${formatCents(payload.amount)}`,
    };

    return descriptions[event.event_type] || `${event.event_type} from ${event.service}`;
}

/**
 * Format cents to dollar string (e.g., 1998 → "$19.98")
 */
function formatCents(cents) {
    if (!cents && cents !== 0) return '$0.00';
    return `$${(cents / 100).toFixed(2)}`;
}

/**
 * Format duration in milliseconds to human-readable string
 */
function formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
}

module.exports = { createTimelineRoutes };
