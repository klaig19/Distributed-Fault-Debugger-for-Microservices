#!/usr/bin/env node
// =============================================================================
// ONE-COMMAND STARTUP — cross-platform (Ubuntu + Windows)
// =============================================================================
// Brings up the ENTIRE system with Docker: infra (Kafka, Zookeeper, Postgres,
// Redis) + all 5 microservices, waits until everything is healthy, optionally
// seeds demo data, then prints every URL.
//
//   npm run start:all            # start + wait + seed
//   npm run start:all -- --no-seed
//
// Pure Node so it behaves identically in bash, PowerShell, and CMD.
// =============================================================================
const { spawnSync } = require('child_process');
const http = require('http');

const SEED = !process.argv.includes('--no-seed');
const log = (m) => console.log(`\x1b[36m[start]\x1b[0m ${m}`);
const err = (m) => console.error(`\x1b[31m[start]\x1b[0m ${m}`);

// `docker compose` (v2) with fallback to legacy `docker-compose`.
function compose(args, opts = {}) {
    let r = spawnSync('docker', ['compose', ...args], { stdio: 'inherit', shell: true, ...opts });
    if (r.error || r.status === null) {
        r = spawnSync('docker-compose', args, { stdio: 'inherit', shell: true, ...opts });
    }
    return r;
}

function dockerPresent() {
    const r = spawnSync('docker', ['--version'], { stdio: 'ignore', shell: true });
    return !r.error && r.status === 0;
}

function waitForHttp(url, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    return new Promise((resolve) => {
        const tick = () => {
            const req = http.get(url, (res) => {
                res.resume();
                if (res.statusCode && res.statusCode < 500) return resolve(true);
                retry();
            });
            req.on('error', retry);
            req.setTimeout(3000, () => req.destroy());
        };
        const retry = () => (Date.now() > deadline ? resolve(false) : setTimeout(tick, 3000));
        tick();
    });
}

(async () => {
    if (!dockerPresent()) {
        err('Docker not found. Install Docker Engine (Ubuntu) or Docker Desktop (Windows).');
        err('See INSTALL.md. Then re-run:  npm run start:all');
        process.exit(1);
    }

    log('Building images and starting infra + all services...');
    const up = compose(['up', '-d', '--build']);
    if (up.status !== 0) { err('docker compose up failed. See output above.'); process.exit(1); }

    log('Waiting for Debug API health (http://localhost:3005/health) — up to 3 min...');
    const ok = await waitForHttp('http://localhost:3005/health', 180000);
    if (!ok) {
        err('Debug API did not become healthy in time. Inspect:  docker compose logs');
        process.exit(1);
    }
    log('Debug API is up.');

    if (SEED) {
        log('Seeding demo data...');
        const seed = spawnSync(process.execPath, ['scripts/seed-events.js'], { stdio: 'inherit' });
        if (seed.status !== 0) err('Seed failed (non-fatal). You can retry:  npm run seed');
    }

    console.log('\n\x1b[32m========================================\x1b[0m');
    console.log('\x1b[32m  System is UP\x1b[0m');
    console.log('\x1b[32m========================================\x1b[0m');
    console.log('  Debug UI + API : http://localhost:3005');
    console.log('  Order Service  : http://localhost:3001/health');
    console.log('  Payment Service: http://localhost:3002/health');
    console.log('  Event Ingestor : http://localhost:3003/health');
    console.log('  Replay Engine  : http://localhost:3004/health');
    console.log('  Adminer (DB)   : http://localhost:8080');
    console.log('\n  Health check all:  npm run health');
    console.log('  Stop everything :  npm run stop:all');
})();
