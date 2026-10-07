#!/usr/bin/env node
// =============================================================================
// HEALTH CHECK — every component, cross-platform
// =============================================================================
// Probes each microservice's /health endpoint and the infra containers.
//   npm run health
// Exit code 0 only if ALL components report healthy.
// =============================================================================
const http = require('http');
const { spawnSync } = require('child_process');

const SERVICES = [
    ['order-service',   'http://localhost:3001/health'],
    ['payment-service', 'http://localhost:3002/health'],
    ['event-ingestor',  'http://localhost:3003/health'],
    ['replay-engine',   'http://localhost:3004/health'],
    ['debug-api',       'http://localhost:3005/health'],
];

function probe(url) {
    return new Promise((resolve) => {
        const req = http.get(url, (res) => {
            let body = '';
            res.on('data', (c) => (body += c));
            res.on('end', () => resolve({ ok: res.statusCode === 200, code: res.statusCode, body }));
        });
        req.on('error', (e) => resolve({ ok: false, code: 0, body: e.code || 'ERR' }));
        req.setTimeout(3000, () => { req.destroy(); resolve({ ok: false, code: 0, body: 'TIMEOUT' }); });
    });
}

function infraHealth() {
    // Ask Docker for container health where available.
    const checks = [
        ['kafka',     ['exec', '-T', 'kafka', 'kafka-broker-api-versions', '--bootstrap-server', 'localhost:9092']],
        ['postgres',  ['exec', '-T', 'postgres', 'pg_isready', '-U', 'replay_user', '-d', 'chronoscope']],
        ['redis',     ['exec', '-T', 'redis', 'redis-cli', 'ping']],
        ['zookeeper', ['exec', '-T', 'zookeeper', 'bash', '-c', 'echo ruok | nc localhost 2181']],
    ];
    const results = [];
    for (const [name, args] of checks) {
        let r = spawnSync('docker', ['compose', ...args], { encoding: 'utf8', shell: true });
        if (r.error || r.status === null) r = spawnSync('docker-compose', args, { encoding: 'utf8', shell: true });
        results.push([name, r.status === 0]);
    }
    return results;
}

(async () => {
    let allOk = true;
    console.log('\n=== INFRA ===');
    for (const [name, ok] of infraHealth()) {
        allOk = allOk && ok;
        console.log(`  ${ok ? '\x1b[32mOK  \x1b[0m' : '\x1b[31mDOWN\x1b[0m'}  ${name}`);
    }

    console.log('\n=== SERVICES ===');
    for (const [name, url] of SERVICES) {
        const r = await probe(url);
        allOk = allOk && r.ok;
        console.log(`  ${r.ok ? '\x1b[32mOK  \x1b[0m' : '\x1b[31mDOWN\x1b[0m'}  ${name.padEnd(16)} ${url}  (${r.code || r.body})`);
    }

    console.log(`\n${allOk ? '\x1b[32mAll components healthy\x1b[0m' : '\x1b[31mSome components are down — see above\x1b[0m'}\n`);
    process.exit(allOk ? 0 : 1);
})();
