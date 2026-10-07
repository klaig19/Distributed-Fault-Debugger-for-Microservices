// =============================================================================
// DATABASE INITIALIZER — PostgreSQL Schema Setup
// =============================================================================
// This script reads the init-db.sql file and executes it against the 
// local PostgreSQL instance. It ensures the environment is ready for
// the Event Store and Replay Engine.
// =============================================================================

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'chronoscope',
    user: 'replay_user',
    password: 'replay_pass',
});

async function initDb() {
    try {
        console.log('Connecting to PostgreSQL...');
        const sqlPath = path.join(__dirname, 'init-db.sql');
        const sql = fs.readFileSync(sqlPath, 'utf8');

        // Split by semicolon to execute commands (basic splitting)
        // Note: For complex SQL with procedures, this might need refinement
        console.log('Executing schema initialization...');
        await pool.query(sql);
        
        console.log('Database schema initialized successfully!');
    } catch (error) {
        console.error('Failed to initialize database:', error.message);
        if (error.message.includes('database "chronoscope" does not exist')) {
            console.log('Note: The docker-compose setup should create the database automatically.');
        }
    } finally {
        await pool.end();
    }
}

initDb();
