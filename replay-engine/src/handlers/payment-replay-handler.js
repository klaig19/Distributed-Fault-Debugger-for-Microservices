// =============================================================================
// PAYMENT REPLAY HANDLER — Replay Logic for Payment Events
// =============================================================================
// This handler replays payment events with EXTRA CARE for side effects.
//
// PAYMENT EVENTS HAVE THE MOST DANGEROUS SIDE EFFECTS:
//   - PAYMENT_INITIATED  → Would call external payment gateway
//   - PAYMENT_SUCCESS    → Would trigger email + webhook notifications
//   - PAYMENT_FAILED     → Would trigger retry logic + customer alerts
//   - PAYMENT_REFUNDED   → Would call refund API on payment gateway
//
// ALL of these external calls are COMPLETELY DISABLED during replay.
// The handler only processes the STATE CHANGE, not the side effect.
//

// =============================================================================

const { createLogger } = require('@chronoscope/core');

// =============================================================================
// createPaymentReplayHandler() — Factory function
// =============================================================================
// PARAMETERS:
//   @param {function} getPool — Returns the shared pg Pool (lazy: the pool
//                               is created by the replayer's connect())
function createPaymentReplayHandler(getPool) {
    const logger = createLogger('payment-replay-handler');

    // Upsert the full payment row (used by PAYMENT_INITIATED)
    const UPSERT_PAYMENT = `
        INSERT INTO payments (payment_id, order_id, amount, status, payment_method, correlation_id, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
        ON CONFLICT (payment_id) DO UPDATE
        SET order_id = EXCLUDED.order_id,
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            payment_method = EXCLUDED.payment_method,
            updated_at = EXCLUDED.updated_at
    `;

    // Status-only transition (SUCCESS / FAILED / REFUNDED). Upsert so a
    // rebuild works even if PAYMENT_INITIATED fell outside the replay window.
    const UPSERT_PAYMENT_STATUS = `
        INSERT INTO payments (payment_id, order_id, status, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (payment_id) DO UPDATE
        SET status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
    `;

    return {
        // =====================================================================
        // handlePaymentInitiated() — Replay PAYMENT_INITIATED event
        // =====================================================================
        // ORIGINAL BEHAVIOR:
        //   1. Create payment record
        //   2. Call payment gateway API ← SIDE EFFECT (BLOCKED IN REPLAY)
        //
        // REPLAY BEHAVIOR:
        //   dry-run: Validate, describe what would happen
        //   state-rebuild: Create payment record WITHOUT calling gateway
        // =====================================================================
        async handlePaymentInitiated(event, mode) {
            const { paymentId, orderId, amount, method, status } = event.payload;

            logger.info('Replaying PAYMENT_INITIATED', {
                mode,
                paymentId,
                orderId,
                amount,
                method,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'INITIATE_PAYMENT',
                    description: `Would initiate ${method} payment of ${amount} cents for order ${orderId}`,
                    details: {
                        paymentId,
                        orderId,
                        amount,
                        method,
                        status: 'INITIATED',
                        stateChange: 'New payment record would be created',
                    },
                    // Explicitly list side effects that are BLOCKED
                    blockedSideEffects: [
                        'Payment gateway API call (would charge customer)',
                        'Payment processing webhook',
                    ],
                    validation: {
                        hasPaymentId: !!paymentId,
                        hasOrderId: !!orderId,
                        hasValidAmount: amount > 0,
                        hasMethod: !!method,
                        isValid: !!paymentId && !!orderId && amount > 0,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                const createdAt = new Date(parseInt(event.timestamp, 10)).toISOString();

                await getPool().query(UPSERT_PAYMENT, [
                    paymentId,
                    orderId,
                    amount,
                    'INITIATED',
                    method || 'CREDIT_CARD',
                    event.correlation_id,
                    createdAt,
                ]);

                return {
                    action: 'INITIATE_PAYMENT',
                    description: `Payment ${paymentId} record rebuilt (INITIATED)`,
                    stateCreated: {
                        paymentId,
                        orderId,
                        amount,
                        method,
                        status: 'INITIATED',
                        correlationId: event.correlation_id,
                        createdAt,
                    },
                    blockedSideEffects: ['Payment gateway API call'],
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handlePaymentSuccess() — Replay PAYMENT_SUCCESS event
        // =====================================================================
        // ORIGINAL BEHAVIOR:
        //   1. Update payment status to SUCCESS
        //   2. Send confirmation email ← SIDE EFFECT (BLOCKED)
        //   3. Trigger webhook ← SIDE EFFECT (BLOCKED)
        //   4. Update order status to PAID
        //
        // REPLAY BEHAVIOR:
        //   Only processes state change (steps 1, 4).
        //   Steps 2, 3 are explicitly blocked.
        //
      
        // =====================================================================
        async handlePaymentSuccess(event, mode) {
            const { paymentId, orderId, amount, transactionRef, gatewayResponse } = event.payload;

            logger.info('Replaying PAYMENT_SUCCESS', {
                mode,
                paymentId,
                orderId,
                amount,
                transactionRef,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'PAYMENT_SUCCESS',
                    description: `Payment ${paymentId} for order ${orderId} would be marked as successful`,
                    details: {
                        paymentId,
                        orderId,
                        amount,
                        transactionRef,
                        gatewayResponse,
                        stateChange: 'Payment status → SUCCESS, Order status → PAID',
                    },
                    // CRITICAL: List all blocked side effects
                    blockedSideEffects: [
                        'Confirmation email to customer (would send duplicate)',
                        'Payment success webhook (would trigger duplicate actions)',
                        'SMS notification (would confuse customer)',
                        'Inventory reservation confirmation',
                    ],
                    validation: {
                        hasPaymentId: !!paymentId,
                        hasOrderId: !!orderId,
                        hasTransactionRef: !!transactionRef,
                        isValid: !!paymentId && !!orderId,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                const updatedAt = new Date(parseInt(event.timestamp, 10)).toISOString();

                await getPool().query(UPSERT_PAYMENT_STATUS, [paymentId, orderId, 'SUCCESS', updatedAt]);

                return {
                    action: 'PAYMENT_SUCCESS',
                    description: `Payment ${paymentId} state rebuilt (SUCCESS)`,
                    stateUpdated: {
                        paymentId,
                        orderId,
                        status: 'SUCCESS',
                        transactionRef,
                        updatedAt,
                    },
                    blockedSideEffects: [
                        'Confirmation email',
                        'Webhook notification',
                        'SMS alert',
                    ],
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handlePaymentFailed() — Replay PAYMENT_FAILED event
        // =====================================================================
        // ORIGINAL BEHAVIOR:
        //   1. Update payment status to FAILED
        //   2. Send failure notification ← SIDE EFFECT (BLOCKED)
        //   3. Trigger retry logic ← SIDE EFFECT (BLOCKED)
        //
        // REPLAY: Only marks payment as failed, no notifications or retries.
        // =====================================================================
        async handlePaymentFailed(event, mode) {
            const { paymentId, orderId, amount, error, errorCode } = event.payload;

            logger.info('Replaying PAYMENT_FAILED', {
                mode,
                paymentId,
                orderId,
                error,
                errorCode,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'PAYMENT_FAILED',
                    description: `Payment ${paymentId} for order ${orderId} failed: ${error}`,
                    details: {
                        paymentId,
                        orderId,
                        amount,
                        error,
                        errorCode,
                        stateChange: 'Payment status → FAILED',
                    },
                    blockedSideEffects: [
                        'Failure notification email (would alarm customer unnecessarily)',
                        'Payment retry mechanism (would attempt to re-charge)',
                        'Alert webhook to monitoring systems',
                    ],
                    // Special: flag this as a failure event for the timeline UI
                    isFailure: true,
                    failureDetails: {
                        error,
                        errorCode,
                        impact: 'Order will not be fulfilled without successful payment',
                    },
                    validation: {
                        hasPaymentId: !!paymentId,
                        hasOrderId: !!orderId,
                        hasError: !!error,
                        isValid: !!paymentId && !!orderId,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                const updatedAt = new Date(parseInt(event.timestamp, 10)).toISOString();

                await getPool().query(UPSERT_PAYMENT_STATUS, [paymentId, orderId, 'FAILED', updatedAt]);

                return {
                    action: 'PAYMENT_FAILED',
                    description: `Payment ${paymentId} state rebuilt (FAILED)`,
                    stateUpdated: {
                        paymentId,
                        orderId,
                        status: 'FAILED',
                        error,
                        errorCode,
                        updatedAt,
                    },
                    isFailure: true,
                    blockedSideEffects: ['Retry mechanism', 'Notification emails'],
                    applied: true,
                };
            }
        },

        // =====================================================================
        // handlePaymentRefunded() — Replay PAYMENT_REFUNDED event
        // =====================================================================
        // ORIGINAL BEHAVIOR:
        //   1. Call refund API on payment gateway ← SIDE EFFECT (BLOCKED)
        //   2. Update payment status to REFUNDED
        //   3. Send refund confirmation email ← SIDE EFFECT (BLOCKED)
        //
        // REPLAY: Only marks payment as refunded.
        // Calling the refund API during replay would DOUBLE-REFUND.
        // =====================================================================
        async handlePaymentRefunded(event, mode) {
            const { paymentId, orderId, amount, reason, originalTransactionRef } = event.payload;

            logger.info('Replaying PAYMENT_REFUNDED', {
                mode,
                paymentId,
                orderId,
                amount,
                reason,
                correlation_id: event.correlation_id,
            });

            if (mode === 'dry-run') {
                return {
                    action: 'PAYMENT_REFUNDED',
                    description: `Payment ${paymentId} for order ${orderId} would be marked as refunded`,
                    details: {
                        paymentId,
                        orderId,
                        amount,
                        reason,
                        originalTransactionRef,
                        stateChange: 'Payment status → REFUNDED',
                    },
                    blockedSideEffects: [
                        'Refund API call to payment gateway (would double-refund!)',
                        'Refund confirmation email',
                        'Refund webhook notification',
                    ],
                    validation: {
                        hasPaymentId: !!paymentId,
                        hasOrderId: !!orderId,
                        hasAmount: amount > 0,
                        isValid: !!paymentId && !!orderId,
                    },
                };
            }

            if (mode === 'state-rebuild') {
                const updatedAt = new Date(parseInt(event.timestamp, 10)).toISOString();

                await getPool().query(UPSERT_PAYMENT_STATUS, [paymentId, orderId, 'REFUNDED', updatedAt]);

                return {
                    action: 'PAYMENT_REFUNDED',
                    description: `Payment ${paymentId} state rebuilt (REFUNDED)`,
                    stateUpdated: {
                        paymentId,
                        orderId,
                        status: 'REFUNDED',
                        refundReason: reason,
                        updatedAt,
                    },
                    blockedSideEffects: ['Refund API call', 'Refund confirmation email'],
                    applied: true,
                };
            }
        },
    };
}

module.exports = { createPaymentReplayHandler };
