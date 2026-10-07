// =============================================================================
// SEED EVENTS — Populate the Event Store with Demo Data
// =============================================================================
// This script inserts realistic demo events into PostgreSQL for testing
// the Debug UI and Replay Engine without running the full service stack.
//
// It creates multiple request flows:
//   1. A SUCCESSFUL order flow (order → payment → success)
//   2. A FAILED payment flow (order → payment → failure)
//   3. A CANCELLED order flow (order → cancel → refund)
//
// USAGE:
//   node scripts/seed-events.js
//
// PREREQUISITES:
//   - PostgreSQL running (docker-compose up postgres)
//   - Database initialized (init-db.sql executed)
// =============================================================================

const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'chronoscope',
    user: 'replay_user',
    password: 'replay_pass',
});

// =============================================================================
// DEMO EVENT FLOWS
// =============================================================================

function generateSuccessFlow() {
    const correlationId = `req-${uuidv4().substring(0, 8)}`;
    const orderId = `ord-${uuidv4().substring(0, 8)}`;
    const paymentId = `pay-${uuidv4().substring(0, 8)}`;
    const baseTime = Date.now() - Math.floor(Math.random() * 3600000); // Random within last hour

    return [
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'ORDER_CREATED',
            service: 'order-service',
            timestamp: baseTime,
            payload: {
                orderId,
                customerId: `cust-${uuidv4().substring(0, 6)}`,
                items: [
                    { name: 'Mechanical Keyboard', quantity: 1, price: 14999 },
                    { name: 'Mouse Pad XL', quantity: 2, price: 2499 },
                ],
                totalAmount: 19997,
                status: 'PENDING',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_INITIATED',
            service: 'payment-service',
            timestamp: baseTime + 150,
            payload: {
                paymentId,
                orderId,
                amount: 19997,
                method: 'CREDIT_CARD',
                status: 'INITIATED',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_SUCCESS',
            service: 'payment-service',
            timestamp: baseTime + 1200,
            payload: {
                paymentId,
                orderId,
                amount: 19997,
                transactionRef: `txn-${uuidv4().substring(0, 8)}`,
                gatewayResponse: 'APPROVED',
                status: 'SUCCESS',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'ORDER_UPDATED',
            service: 'order-service',
            timestamp: baseTime + 1500,
            payload: {
                orderId,
                previousState: { status: 'PENDING', totalAmount: 19997 },
                currentState: { status: 'PAID', totalAmount: 19997 },
                changes: ['status'],
            },
        },
    ];
}

function generateFailureFlow() {
    const correlationId = `req-${uuidv4().substring(0, 8)}`;
    const orderId = `ord-${uuidv4().substring(0, 8)}`;
    const paymentId = `pay-${uuidv4().substring(0, 8)}`;
    const baseTime = Date.now() - Math.floor(Math.random() * 3600000);

    return [
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'ORDER_CREATED',
            service: 'order-service',
            timestamp: baseTime,
            payload: {
                orderId,
                customerId: `cust-${uuidv4().substring(0, 6)}`,
                items: [
                    { name: 'Ultra Monitor 4K', quantity: 1, price: 89999 },
                ],
                totalAmount: 89999,
                status: 'PENDING',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_INITIATED',
            service: 'payment-service',
            timestamp: baseTime + 200,
            payload: {
                paymentId,
                orderId,
                amount: 89999,
                method: 'CREDIT_CARD',
                status: 'INITIATED',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_FAILED',
            service: 'payment-service',
            timestamp: baseTime + 2500,
            payload: {
                paymentId,
                orderId,
                amount: 89999,
                error: 'Insufficient funds',
                errorCode: 'INSUFFICIENT_FUNDS',
                status: 'FAILED',
            },
        },
    ];
}

function generateCancelledFlow() {
    const correlationId = `req-${uuidv4().substring(0, 8)}`;
    const orderId = `ord-${uuidv4().substring(0, 8)}`;
    const paymentId = `pay-${uuidv4().substring(0, 8)}`;
    const baseTime = Date.now() - Math.floor(Math.random() * 3600000);

    return [
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'ORDER_CREATED',
            service: 'order-service',
            timestamp: baseTime,
            payload: {
                orderId,
                customerId: `cust-${uuidv4().substring(0, 6)}`,
                items: [
                    { name: 'Wireless Earbuds', quantity: 1, price: 7999 },
                    { name: 'Charging Case', quantity: 1, price: 1999 },
                ],
                totalAmount: 9998,
                status: 'PENDING',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_INITIATED',
            service: 'payment-service',
            timestamp: baseTime + 100,
            payload: {
                paymentId,
                orderId,
                amount: 9998,
                method: 'DEBIT_CARD',
                status: 'INITIATED',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_SUCCESS',
            service: 'payment-service',
            timestamp: baseTime + 800,
            payload: {
                paymentId,
                orderId,
                amount: 9998,
                transactionRef: `txn-${uuidv4().substring(0, 8)}`,
                gatewayResponse: 'APPROVED',
                status: 'SUCCESS',
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'ORDER_CANCELLED',
            service: 'order-service',
            timestamp: baseTime + 5000,
            payload: {
                orderId,
                previousStatus: 'PAID',
                reason: 'Customer changed their mind',
                totalAmount: 9998,
            },
        },
        {
            event_id: uuidv4(),
            correlation_id: correlationId,
            event_type: 'PAYMENT_REFUNDED',
            service: 'payment-service',
            timestamp: baseTime + 5500,
            payload: {
                paymentId,
                orderId,
                amount: 9998,
                reason: 'Customer changed their mind',
                originalTransactionRef: `txn-${uuidv4().substring(0, 8)}`,
                status: 'REFUNDED',
            },
        },
    ];
}

// =============================================================================
// MAIN — Insert seed data
// =============================================================================
async function seed() {
    console.log('Seeding event store with demo data...\n');

    const allEvents = [];

    // Generate 5 successful flows
    for (let i = 0; i < 5; i++) {
        allEvents.push(...generateSuccessFlow());
    }

    // Generate 3 failed flows
    for (let i = 0; i < 3; i++) {
        allEvents.push(...generateFailureFlow());
    }

    // Generate 2 cancelled flows
    for (let i = 0; i < 2; i++) {
        allEvents.push(...generateCancelledFlow());
    }

    // Insert all events
    let insertedCount = 0;
    for (const event of allEvents) {
        try {
            await pool.query(`
                INSERT INTO events (event_id, correlation_id, event_type, service, timestamp, payload, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (event_id) DO NOTHING
            `, [
                event.event_id,
                event.correlation_id,
                event.event_type,
                event.service,
                event.timestamp,
                JSON.stringify(event.payload),
                JSON.stringify({ source: 'seed-script' }),
            ]);
            insertedCount++;
        } catch (error) {
            console.error(`Failed to insert event: ${error.message}`);
        }
    }

    console.log(`Inserted ${insertedCount} events across ${10} flows`);
    console.log(`   - 5 successful payment flows`);
    console.log(`   - 3 failed payment flows`);
    console.log(`   - 2 cancelled order flows\n`);

    // Print some correlation IDs for testing
    const correlationIds = [...new Set(allEvents.map(e => e.correlation_id))];
    console.log('Correlation IDs for testing:');
    correlationIds.forEach(id => console.log(`   ${id}`));

    await pool.end();
    console.log('\n Seeding complete!');
}

seed().catch(console.error);
