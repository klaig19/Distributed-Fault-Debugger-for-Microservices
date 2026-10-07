const { Pool } = require('pg');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'chronoscope',
    user: 'replay_user',
    password: 'replay_pass',
});

async function clear() {
    console.log('Clearing all data from database...');
    try {
        await pool.query('TRUNCATE TABLE events, processed_events, replay_sessions, orders, payments CASCADE');
        console.log('Database completely cleared!');
    } catch (error) {
        console.error('Failed to clear database:', error.message);
    } finally {
        await pool.end();
    }
}

clear();
