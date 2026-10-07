# =============================================================================
# Chronoscope — Application Image
# =============================================================================
# ONE image for ALL five microservices. Each service is launched by overriding
# the container `command` (see docker-compose.yml), e.g.:
#     command: ["node", "debug-api/src/index.js"]
#
# Rationale: the services share the same npm workspace tree and @chronoscope/core
# dependency, so a single build stage keeps the image cache warm and the repo
# simple. This is cross-platform (Docker Desktop on Windows, Docker Engine on
# Ubuntu) — no OS-specific instructions.
# =============================================================================
FROM node:22-alpine

# Tini gives us proper PID-1 signal handling so Ctrl+C / `docker stop`
# terminates the Node process cleanly instead of leaking it.
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

ENV NODE_ENV=production
WORKDIR /app

# ---------------------------------------------------------------------------
# Dependency layer — copy every workspace manifest first so `npm ci` is cached
# and only re-runs when a package.json / lockfile actually changes.
# ---------------------------------------------------------------------------
COPY package.json package-lock.json ./
COPY core/package.json ./core/
COPY services/order-service/package.json ./services/order-service/
COPY services/payment-service/package.json ./services/payment-service/
COPY event-ingestor/package.json ./event-ingestor/
COPY replay-engine/package.json ./replay-engine/
COPY debug-api/package.json ./debug-api/

RUN npm ci --omit=dev

# ---------------------------------------------------------------------------
# Source layer
# ---------------------------------------------------------------------------
COPY . .

# Drop root privileges (node:alpine ships an unprivileged `node` user).
USER node

# Default command runs the Debug API (also serves the frontend UI on 3005).
# docker-compose overrides this per service.
CMD ["node", "debug-api/src/index.js"]
