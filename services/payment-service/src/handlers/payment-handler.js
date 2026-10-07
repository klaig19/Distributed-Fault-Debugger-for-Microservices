// =============================================================================
// PAYMENT HANDLER — Business Logic with Side-Effect Isolation
// =============================================================================
// This handler demonstrates the CRITICAL concept of side-effect isolation
// for the replay engine.
//
// SIDE EFFECTS IN THIS SERVICE:
//   1. Payment gateway API call (charge customer's card)
//   2. Email notification (send payment confirmation)
//   3. SMS notification (send payment alert)
//   4. Webhook trigger (notify external systems)
//
// ALL of these are EXTERNAL side effects that MUST be disabled during replay.
//
// REPLAY SAFETY PATTERN:
//   if (!this.replayMode) {
//       await this.callPaymentGateway(paymentDetails);  // Real call
//   } else {
//       logger.info('🔇 [REPLAY] Skipping payment gateway call');
//       // Return simulated success
//   }
//

//

// =============================================================================

const { v4: uuidv4 } = require('uuid');
const { createEvent, EventTypes, createLogger } = require('@chronoscope/core');

// =============================================================================
// createPaymentHandler() — Factory function for payment business logic
// =============================================================================
// PARAMETERS:
//   @param {object} producer    — Kafka producer for emitting events
//   @param {string} serviceName — Name of this service
//   @param {object} options     — Configuration options
//     - replayMode: boolean     → If true, disable all side effects
//     - paymentDelayMs: number  → Simulated payment processing time
//     - failureRate: number     → Probability of payment failure (0-1)
//
// RETURNS:
//   Object with methods: onOrderCreated, onOrderCancelled
// =============================================================================
function createPaymentHandler(producer, serviceName, options = {}) {
    const logger = createLogger(serviceName);

    // =========================================================================
    // CONFIGURATION
    // =========================================================================
    const {
        replayMode = false,            // Side-effect isolation flag
        paymentDelayMs = 1000,         // Simulate processing delay
        failureRate = 0.1,             // 10% failure rate for testing
    } = options;

    // =========================================================================
    // IN-MEMORY PAYMENT STORE
    // =========================================================================
    // Tracks payment state. In production, this is PostgreSQL.
    // =========================================================================
    const payments = new Map();

    // =========================================================================
    // PENDING CANCELLATIONS — closes the cancel-vs-payment race
    // =========================================================================
    // If ORDER_CANCELLED arrives while the payment gateway call is still in
    // flight, the cancellation handler finds no SUCCESS payment and would
    // previously just return — leaving the customer charged with no refund
    // once the gateway call completed. We record the cancellation here so
    // onOrderCreated can refund immediately after a late success.
    // KEY: orderId → cancellation reason
    // =========================================================================
    const cancelledOrders = new Map();

    // =========================================================================
    // SIMULATED EXTERNAL SERVICES
    // =========================================================================
    // These functions simulate real external service calls.
    // In production, these would call Stripe, Twilio, SendGrid, etc.
    // During replay, these are COMPLETELY SKIPPED.
    // =========================================================================

    /**
     * Simulates calling a payment gateway API (e.g., Stripe, PayPal)
     * 
     * In production:
     *   - This would call the Stripe Charges API
     *   - Handle 3D Secure authentication
     *   - Process the actual credit card charge
     * 
     * SIDE EFFECT: Charges real money from customer's account
     * REPLAY BEHAVIOR: MUST be skipped — would double-charge customer
     */
    async function callPaymentGateway(paymentDetails) {
        // Simulate API latency
        await new Promise(resolve => setTimeout(resolve, paymentDelayMs));

        // Simulate random failures for testing, OR guaranteed failure if requested
        const isIntentionalFailure = paymentDetails.customerId && paymentDetails.customerId.startsWith('fail-');
        const shouldFail = isIntentionalFailure || (Math.random() < failureRate);

        if (shouldFail) {
            return {
                success: false,
                error: 'Insufficient funds',
                errorCode: 'INSUFFICIENT_FUNDS',
                transactionRef: null,
            };
        }

        return {
            success: true,
            transactionRef: `txn-${uuidv4().substring(0, 8)}`,
            gatewayResponse: 'APPROVED',
            processingTime: paymentDelayMs,
        };
    }

    /**
     * Simulates sending a payment confirmation email
     * 
     * SIDE EFFECT: Sends a real email to the customer
     * REPLAY BEHAVIOR: MUST be skipped — would send duplicate emails
     */
    async function sendPaymentEmail(customerId, paymentDetails) {
        logger.info('Sending payment confirmation email', {
            customerId,
            amount: paymentDetails.amount,
        });
        // In production: await sendGrid.send({ to: customer.email, ... })
    }

    /**
     * Simulates sending a webhook to external systems
     * 
     * SIDE EFFECT: Triggers actions in external systems
     * REPLAY BEHAVIOR: MUST be skipped — would trigger duplicate actions
     */
    async function triggerWebhook(event) {
        logger.info('Triggering webhook', {
            event_type: event.event_type,
            correlation_id: event.correlation_id,
        });
        // In production: await axios.post(webhookUrl, event)
    }

    /**
     * Refunds a successful payment: calls the refund API (skipped in replay
     * mode), updates the payment record, and emits PAYMENT_REFUNDED.
     * Shared by onOrderCancelled and the cancel-vs-payment race path in
     * onOrderCreated.
     */
    async function processRefund(payment, reason, correlationId) {
        if (!replayMode) {
            logger.info('Processing refund...', {
                paymentId: payment.paymentId,
                amount: payment.amount,
            });
            // In production: await stripe.refunds.create({ charge: payment.transactionRef })
            await new Promise(resolve => setTimeout(resolve, 500));
        } else {
            logger.info('[REPLAY MODE] Skipping refund API call', {
                paymentId: payment.paymentId,
                amount: payment.amount,
            });
        }

        payment.status = 'REFUNDED';
        payment.refundedAt = new Date().toISOString();
        payment.updatedAt = new Date().toISOString();
        payments.set(payment.paymentId, payment);

        const refundEvent = createEvent(
            EventTypes.PAYMENT_REFUNDED,
            correlationId,
            serviceName,
            {
                paymentId: payment.paymentId,
                orderId: payment.orderId,
                amount: payment.amount,
                reason,
                originalTransactionRef: payment.transactionRef,
                status: 'REFUNDED',
            }
        );

        await producer.emit(refundEvent);

        logger.info('Payment refunded', {
            paymentId: payment.paymentId,
            orderId: payment.orderId,
            amount: payment.amount,
            correlation_id: correlationId,
        });
    }

    return {
        // =====================================================================
        // onOrderCreated() — Handle ORDER_CREATED event
        // =====================================================================
        // This is the PRIMARY event handler. Triggered when a new order is
        // placed by the Order Service.
        //
        // FLOW:
        //   1. Extract order details from event payload
        //   2. Create payment record (INITIATED status)
        //   3. Emit PAYMENT_INITIATED event
        //   4. Process payment (call gateway OR skip in replay mode)
        //   5. Update payment record (SUCCESS or FAILED)
        //   6. Emit PAYMENT_SUCCESS or PAYMENT_FAILED event
        //   7. Send notifications (email, webhook — skipped in replay)
        //
        // SIDE-EFFECT ISOLATION:
        //   Steps 4, 7 contain side effects and are gated by replayMode.
        //   Steps 1-3, 5-6 are pure state changes and always execute.
        //   This allows replay to verify state transitions without
        //   triggering real-world consequences.
        //
        // PARAMETERS:
        //   @param {object} event — The ORDER_CREATED event from Kafka
        // =====================================================================
        async onOrderCreated(event) {
            const { orderId, customerId, totalAmount, items } = event.payload;
            const correlationId = event.correlation_id;

            logger.info('Processing payment for order', {
                orderId,
                customerId,
                totalAmount,
                correlation_id: correlationId,
                replayMode,
            });

            // -----------------------------------------------------------------
            // Step 1: Create payment record
            // -----------------------------------------------------------------
            const paymentId = `pay-${uuidv4().substring(0, 8)}`;
            const payment = {
                paymentId,
                orderId,
                customerId,
                amount: totalAmount,
                status: 'INITIATED',
                method: 'CREDIT_CARD',
                correlationId,
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
            };

            payments.set(paymentId, payment);

            // -----------------------------------------------------------------
            // Step 2: Emit PAYMENT_INITIATED event
            // -----------------------------------------------------------------
            // This event signals that payment processing has started.
            // Downstream systems can use this to show "Payment in progress".
            // -----------------------------------------------------------------
            const initiatedEvent = createEvent(
                EventTypes.PAYMENT_INITIATED,
                correlationId,
                serviceName,
                {
                    paymentId,
                    orderId,
                    amount: totalAmount,
                    method: 'CREDIT_CARD',
                    status: 'INITIATED',
                }
            );

            await producer.emit(initiatedEvent);

            logger.info('PAYMENT_INITIATED event emitted', {
                event_id: initiatedEvent.event_id,
                paymentId,
                orderId,
                correlation_id: correlationId,
            });

            // -----------------------------------------------------------------
            // Step 3: Process payment (WITH SIDE-EFFECT ISOLATION)
            // -----------------------------------------------------------------
            
            // In normal mode: Call the real payment gateway
            // In replay mode: Skip the call, simulate success
            //
            // The replayMode flag is injected via dependency injection,
            // not checked from a global variable. This makes the handler:
            //   1. Testable (pass replayMode: true in tests)
            //   2. Configurable (set via environment variable)
            //   3. Explicit (clear what's happening and why)
            // -----------------------------------------------------------------
            let gatewayResult;

            if (!replayMode) {
                // =============================================================
                // NORMAL MODE — Call the real payment gateway
                // =============================================================
                // In production, this would call Stripe's /v1/charges endpoint
                // or similar. This is the SIDE EFFECT we need to isolate.
                // =============================================================
                logger.info('Calling payment gateway...', { paymentId, amount: totalAmount });

                gatewayResult = await callPaymentGateway({
                    paymentId,
                    amount: totalAmount,
                    customerId,
                    method: 'CREDIT_CARD',
                });
            } else {
                // =============================================================
                // REPLAY MODE — Skip the payment gateway call
                // =============================================================
                // During replay, we DON'T call the real gateway.
                // Instead, we simulate a successful response.
                //
                // WHY SIMULATE SUCCESS?
                //   - The replay engine will verify the state transition
                //   - We need the handler to produce the expected next event
                //   - The actual gateway result doesn't matter for state verification
                //
                // IMPORTANT: This is logged so engineers can see what was skipped
                // =============================================================
                logger.info('[REPLAY MODE] Skipping payment gateway call', {
                    paymentId,
                    orderId,
                    amount: totalAmount,
                    reason: 'Side-effect isolation — would charge customers real money',
                });

                gatewayResult = {
                    success: true,
                    transactionRef: `replay-txn-${uuidv4().substring(0, 8)}`,
                    gatewayResponse: 'SIMULATED_REPLAY',
                    processingTime: 0,
                };
            }

            // -----------------------------------------------------------------
            // Step 4: Update payment record based on gateway result
            // -----------------------------------------------------------------
            // This state change happens in BOTH normal and replay mode.
            // It's a pure state transition — no external side effects.
            // -----------------------------------------------------------------
            if (gatewayResult.success) {
                // ============= PAYMENT SUCCESS =============
                payment.status = 'SUCCESS';
                payment.transactionRef = gatewayResult.transactionRef;
                payment.updatedAt = new Date().toISOString();
                payments.set(paymentId, payment);

                // Emit PAYMENT_SUCCESS event
                const successEvent = createEvent(
                    EventTypes.PAYMENT_SUCCESS,
                    correlationId,
                    serviceName,
                    {
                        paymentId,
                        orderId,
                        amount: totalAmount,
                        transactionRef: gatewayResult.transactionRef,
                        gatewayResponse: gatewayResult.gatewayResponse,
                        status: 'SUCCESS',
                    }
                );

                await producer.emit(successEvent);

                logger.info('Payment successful', {
                    paymentId,
                    orderId,
                    transactionRef: gatewayResult.transactionRef,
                    correlation_id: correlationId,
                });

                // ---------------------------------------------------------
                // Step 5: Post-payment notifications (SIDE EFFECTS)
                // ---------------------------------------------------------
                // These are ALSO gated by replayMode.
                // Email, SMS, webhooks — all external side effects.
                // ---------------------------------------------------------
                if (!replayMode) {
                    await sendPaymentEmail(customerId, { amount: totalAmount });
                    await triggerWebhook(successEvent);
                } else {
                    logger.info('[REPLAY MODE] Skipping post-payment notifications', {
                        skipped: ['email', 'webhook'],
                    });
                }

                // ---------------------------------------------------------
                // Cancel-vs-payment race check
                // ---------------------------------------------------------
                // The order may have been cancelled while the gateway call
                // was in flight. If so, refund immediately — otherwise the
                // charge would be stranded (cancel handler already ran and
                // found nothing to refund).
                // ---------------------------------------------------------
                if (cancelledOrders.has(orderId)) {
                    const reason = cancelledOrders.get(orderId);
                    cancelledOrders.delete(orderId);

                    logger.warn('Order was cancelled during payment processing — refunding', {
                        paymentId,
                        orderId,
                        correlation_id: correlationId,
                    });

                    await processRefund(payment, reason, correlationId);
                }

            } else {
                // ============= PAYMENT FAILED =============
                payment.status = 'FAILED';
                payment.error = gatewayResult.error;
                payment.errorCode = gatewayResult.errorCode;
                payment.updatedAt = new Date().toISOString();
                payments.set(paymentId, payment);

                // Emit PAYMENT_FAILED event
                const failedEvent = createEvent(
                    EventTypes.PAYMENT_FAILED,
                    correlationId,
                    serviceName,
                    {
                        paymentId,
                        orderId,
                        amount: totalAmount,
                        error: gatewayResult.error,
                        errorCode: gatewayResult.errorCode,
                        status: 'FAILED',
                    }
                );

                await producer.emit(failedEvent);

                logger.error('Payment failed', {
                    paymentId,
                    orderId,
                    error: gatewayResult.error,
                    errorCode: gatewayResult.errorCode,
                    correlation_id: correlationId,
                });
            }
        },

        // =====================================================================
        // onOrderCancelled() — Handle ORDER_CANCELLED event
        // =====================================================================
        // When an order is cancelled, check if payment was already made.
        // If yes, initiate refund process.
        //
        // FLOW:
        //   1. Find the payment for this order
        //   2. If payment exists and was successful → initiate refund
        //   3. Emit PAYMENT_REFUNDED event
        //
        // SIDE-EFFECT ISOLATION:
        //   Refund API call is gated by replayMode.
        // =====================================================================
        async onOrderCancelled(event) {
            const { orderId, reason } = event.payload;
            const correlationId = event.correlation_id;

            logger.info('Processing order cancellation for payment', {
                orderId,
                reason,
                correlation_id: correlationId,
            });

            // Find the payment for this order
            let existingPayment = null;
            for (const payment of payments.values()) {
                if (payment.orderId === orderId && payment.status === 'SUCCESS') {
                    existingPayment = payment;
                    break;
                }
            }

            if (!existingPayment) {
                // No successful payment YET — but one may be in flight
                // (gateway call takes ~1s). Record the cancellation so the
                // success path in onOrderCreated refunds the late charge
                // instead of stranding the money.
                cancelledOrders.set(orderId, reason);
                logger.info('No successful payment found for cancelled order — armed late-refund guard', {
                    orderId,
                    correlation_id: correlationId,
                });
                return;
            }

            await processRefund(existingPayment, reason, correlationId);
        },
    };
}

module.exports = { createPaymentHandler };
