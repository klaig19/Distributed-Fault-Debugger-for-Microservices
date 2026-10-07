# Chronoscope for Distributed Systems

**Replay any failed production transaction, step by step, with zero side effects — to reproduce and verify fixes without ever touching live infrastructure.**

![Node.js](https://img.shields.io/badge/Node.js-20+-339933?logo=node.js&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Log-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Event%20Store-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Idempotency%20Cache-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

A production-grade debugging framework for distributed microservices. It captures every event in a distributed transaction, stores it in an ordered, queryable event log, and lets engineers **deterministically replay** any failed flow — with full **side-effect isolation** — to safely reproduce and verify fixes without touching live systems.

---

## Highlights

- **10-container distributed system** — 5 Node microservices + Kafka, Zookeeper, PostgreSQL, Redis, Adminer — orchestrated with Docker Compose, verified end-to-end on a clean cold boot.
- **Event-sourced architecture** — the immutable event log is the single source of truth; every state change is captured, traceable, and replayable.
- **Deterministic replay engine** — re-runs historical production events through registered handlers with guaranteed reproducibility.
- **Side-effect isolation** — dependency-injected `replayMode` blocks real payments, emails, and webhooks during replay: zero real-world consequences.
- **Dual-layer idempotency** — Redis hot path + PostgreSQL persistent fallback prevents duplicate processing.
- **Distributed tracing** — correlation IDs stitch every event across every service into one ordered timeline.

**Skills demonstrated:** distributed systems design · event-driven architecture · Apache Kafka · event sourcing · idempotency & exactly-once semantics · Docker orchestration · REST API design · observability & debugging tooling.

---

## The Problem It Solves

In a distributed system, a single user action may touch 5+ services over several seconds. When something breaks, traditional debugging means:

1. Grepping through fragmented logs across multiple machines.
2. Guessing at the sequence of events.
3. Trying to "reproduce" the bug locally — which usually fails because the exact state is gone.

This engine treats **the event log as the source of truth** and lets any past transaction be re-run, step by step, in a controlled environment.

---

## System Architecture

```
                    ┌──────────────────────────┐
                    │     Client / Browser      │
                    └────────────┬─────────────┘
                                 │  HTTP POST /orders
                                 ▼
                    ┌────────────────────────────┐
                    │      Order Service          │
                    │  REST API  |  Event Producer│
                    │       :3001                 │
                    └──────────┬─────────────────┘
                               │ ORDER_CREATED
                               ▼
                    ┌────────────────────────────┐
                    │        Apache Kafka          │
                    │  Durable, Ordered Event Log  │
                    │   orders-events (topic)      │
                    │   payments-events (topic)    │
                    └───┬──────────────┬──────────┘
                        │              │
              EVENT_INGESTOR      PAYMENT_SERVICE
                        │              │
                        ▼              ▼
              ┌──────────────┐  ┌──────────────────┐
              │  PostgreSQL  │  │  Payment Service  │
              │  Event Store │  │  Event Consumer   │
              └──────┬───────┘  │       :3002       │
                     │          └────────┬──────────┘
                     │                   │ PAYMENT_SUCCESS / PAYMENT_FAILED
                     │                   ▼
                     │           (back into Kafka)
                     │                   │
                     │         Order Service consumes
                     │         → Updates order to PAID / CANCELLED
                     │
                     ▼
            ┌──────────────────┐     ┌──────────────────┐
            │    Debug API     │     │  Replay Engine    │
            │  Query + Proxy   │────▶│  Dry-Run + Rebuild│
            │     :3005        │     │      :3004        │
            └──────────────────┘     └──────────────────┘
                     │                        │
                     ▼                        ▼
            ┌──────────────────┐     ┌──────────────────┐
            │  Vanilla JS UI   │     │      Redis        │
            │  Debug Dashboard │     │  Idempotency Cache│
            └──────────────────┘     └──────────────────┘
```

---

## Key Engineering Concepts

### 1. Event-Driven Choreography
Services never call each other directly via HTTP. The `Order Service` publishes events to a Kafka topic and the `Payment Service` subscribes to it. This means:
- Services are **fully decoupled**. If one crashes, the other keeps running.
- Events safely queue up in Kafka and are processed the moment the service recovers.
- The entire flow is **observable** and **replayable** because every state change is captured as an immutable event.

### 2. Distributed Tracing via Correlation IDs
Every incoming HTTP request generates a unique `correlation_id` (e.g., `req-abc-123`). This ID is attached to every Kafka event emitted throughout the entire chain. The result:
- A **complete, ordered timeline** of every event tied to that single request.
- Any event from any service can be traced back to the original user action.
- The Debug UI uses this ID as the primary key for all timeline queries.

### 3. Deterministic Replay
The replay engine fetches all events for a given `correlation_id` from PostgreSQL, ordered exactly by timestamp. It then processes them one-by-one through registered **replay handlers**:
- `ORDER_CREATED` → `orderReplayHandler.handleOrderCreated()`
- `PAYMENT_FAILED` → `paymentReplayHandler.handlePaymentFailed()`

Because the data and the sequence are fixed, the replay is **deterministic** — the same events will always produce the same result.

### 4. Side-Effect Isolation
This is the most critical safety mechanism. During a replay, the engine blocks all real-world operations by injecting a `replayMode: true` flag via dependency injection:
- **Payment Gateway** calls (e.g., Stripe) are skipped — no double-charging.
- **Email notifications** are skipped — no duplicate emails to customers.
- **Webhooks** are skipped — no downstream trigger pollution.

This allows engineers to replay production data against new code changes with **zero real-world consequences**.

### 5. Idempotency (Dual-Layer Strategy)
To prevent duplicate processing during replay, the engine uses two layers:
- **Redis** — O(1) in-memory lookups for high-frequency hot-path checks.
- **PostgreSQL** — Persistent fallback, provides audit trail and survives Redis restarts.

```
Is this event already replayed?
    │
    ├─ Check Redis  → Cache HIT → Skip (fast path)
    │
    └─ Check Postgres → Row exists → Skip (persistent fallback)
                           │
                           └─ Not found → Process event → Mark as processed in both stores
```

---

## Documentation

| Guide | Purpose |
|---|---|
| [INSTALL.md](INSTALL.md) | Full setup — Docker path and host path, Ubuntu + Windows |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Architecture, workspaces, commands, conventions |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Free-tier deployment analysis + recommendation |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common failures and fixes |
| `.env.example` | Every environment variable, documented |

---

## Quick Start

### Prerequisites
- Docker + Docker Compose v2
- Node.js 20+ (22 recommended) — only for the host-dev path and seeding
- npm 9+

### Option A — One command (Docker: infra **and** all 5 services)
```bash
docker compose up --build
```
Open **[http://localhost:3005](http://localhost:3005)**. This is the simplest
path and works identically on Ubuntu and Windows (Docker Desktop). See
[INSTALL.md](INSTALL.md) for details.

### Option B — Host-dev path (infra in Docker, services on host with reload)

### 1. Start Infrastructure
```bash
npm run setup:infra
```
This brings up Apache Kafka, Zookeeper, PostgreSQL, Redis, and Adminer (DB browser), and automatically initializes Kafka topics and the database schema.

### 2. Install Dependencies
```bash
npm install
```

### 3. (Optional) Seed Demo Data
```bash
npm run seed
```
Seeds 20+ realistic event flows — order successes, failures, cancellations, and refunds — so the dashboard has data to explore immediately.

### 4. Start All Services
```bash
npm run dev
```
Starts all 5 services in parallel (Order, Payment, Ingestor, Replay Engine, Debug API).

### 5. Open the Debug Dashboard
Navigate to **[http://localhost:3005](http://localhost:3005)**

---

## Service Ports

| Service | Port | Role |
|---|---|---|
| Order Service | 3001 | REST API + ORDER event producer |
| Payment Service | 3002 | Kafka consumer + PAYMENT event producer |
| Event Ingestor | 3003 | Kafka → PostgreSQL pipeline |
| Replay Engine | 3004 | Dry-run and state rebuild core |
| Debug API + UI | 3005 | Query layer + browser dashboard |
| PostgreSQL | 5432 | Persistent event store |
| Redis | 6379 | Idempotency cache |
| Adminer (DB GUI) | 8080 | Direct database browser |

---

## API Reference

### Order Service (:3001)
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/orders` | Create a new order |
| `GET` | `/orders` | List all orders |
| `GET` | `/orders/:id` | Get order by ID |
| `PUT` | `/orders/:id` | Update an order |
| `DELETE` | `/orders/:id` | Cancel an order |
| `GET` | `/health` | Service health check |

### Debug API (:3005)
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/flows` | All request flows with failure status |
| `GET` | `/stats` | Aggregate dashboard statistics |
| `GET` | `/timeline/:id` | Formatted, step-by-step event timeline |
| `GET` | `/events/correlation/:id` | Raw events for a correlation ID |
| `GET` | `/events/failures` | All events flagged as failures |
| `GET` | `/events/recent` | Most recent events |
| `GET` | `/events/search?q=` | Full-text event search |
| `POST` | `/replay` | Trigger a replay (dry-run or state-rebuild) |

---

## Replay Modes

### Start Dry Run
Re-runs every event for a given flow through the replay handlers with **no state changes** and **no side effects**. Used to:
- **Verify a bug fix** using real production data before deploying.
- **Understand exactly what happened** during a failure without any risk.

### Start State Rebuild
Re-applies every event in sequence to reconstruct the final state in the database. Used to:
- **Recover from data loss** (e.g., after a database crash).
- **Migrate to a new database** by replaying the historical event log.
- **Backfill a new service** that needs historical state it wasn't alive to see.

---

## Live Demo Scenarios

### Scenario 1: Trigger a Successful Order
Open the dashboard, click **"Create Valid Order"**, and watch the following 4-event chain appear:
1.  `ORDER_CREATED` — Order Service accepts the request
2.  `PAYMENT_INITIATED` — Payment Service picks it up from Kafka
3.  `PAYMENT_SUCCESS` — Simulated bank approves the charge
4.  `ORDER_UPDATED` — Order Service receives the confirmation and marks the order `PAID`

### Scenario 2: Trigger a Payment Failure
Click **"Create Failing Order"**. The `customerId` is prefixed with `fail-`, which signals the payment simulator to trigger `INSUFFICIENT_FUNDS`. Watch:
1.  `ORDER_CREATED`
2.  `PAYMENT_INITIATED`
3.  `PAYMENT_FAILED`
4.  `ORDER_CANCELLED` — Order Service reacts to the failure and closes the order

### Scenario 3: Replay a Failure
1. Click on any failed flow in the dashboard.
2. Click **"Start Dry Run"**.
3. The engine fetches the exact events from the database, runs them through the handlers, and reports what the outcome would be — without touching any live system.

---

## Project Structure

```
├── docker-compose.yml               # Full infrastructure stack
├── package.json                     # npm workspaces root
├── scripts/
│   ├── init-db.sql                  # PostgreSQL schema
│   ├── init-db.js                   # Schema initializer
│   ├── init-kafka.js                # Kafka topic creator
│   ├── seed-events.js               # Demo data seeder
│   └── clear-db.js                  # Reset database
├── core/                            # Shared library (@chronoscope/core)
│   ├── event-schema.js              # EventTypes enum + event factory
│   ├── kafka-producer.js            # Kafkajs producer wrapper
│   ├── kafka-consumer.js            # Kafkajs consumer wrapper
│   ├── correlation-middleware.js    # Express req tracing middleware
│   └── logger.js                    # Structured Winston logger
├── services/
│   ├── order-service/
│   │   ├── src/handlers/order-handler.js   # State machine + event emission
│   │   ├── src/routes/orders.js            # REST route definitions
│   │   └── src/index.js                    # Express app + Kafka consumer
│   └── payment-service/
│       ├── src/handlers/payment-handler.js # Payment logic + side-effect isolation
│       └── src/index.js                    # Kafka consumer bootstrap
├── event-ingestor/
│   └── src/index.js                 # Kafka → PostgreSQL ingestion pipeline
├── replay-engine/
│   ├── src/replayer.js              # Core replay loop + session management
│   ├── src/idempotency.js           # Redis + Postgres dual-layer deduplication
│   ├── src/handlers/
│   │   ├── order-replay-handler.js  # Handles ORDER_* events during replay
│   │   └── payment-replay-handler.js# Handles PAYMENT_* events during replay
│   └── src/index.js                 # Express API for triggering replays
├── debug-api/
│   └── src/
│       ├── index.js                 # API entrypoint + static file serving
│       └── routes/
│           ├── events.js            # Event query routes
│           ├── replay.js            # Replay trigger proxy
│           └── timeline.js          # Human-readable timeline formatter
└── frontend/
    └── public/
        ├── index.html               # Dashboard layout
        ├── style.css                # Vanilla CSS design system
        └── app.js                   # All UI logic — flows, timelines, replays
```

---

## Database Access (Adminer)

To inspect or manipulate raw events directly:
1. Start infra: `npm run setup:infra`
2. Navigate to **[http://localhost:8080](http://localhost:8080)**
3. Login with:
   - **System:** PostgreSQL
   - **Server:** postgres
   - **Username:** replay_user
   - **Password:** replay_pass
   - **Database:** chronoscope
