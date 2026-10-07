// =============================================================================
// SETUP KAFKA TOPICS — Infrastructure Initialization
// =============================================================================
// This script ensures all required Kafka topics are created with the correct
// configuration (partitions, replication factor) before services start.
//
// TOPICS CREATED:
//   - orders-events: 3 partitions (allows parallel processing by correlation_id)
//   - payments-events: 3 partitions
// =============================================================================

const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'topic-initializer',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    logLevel: logLevel.NOTHING, // Silence internal Kafka logs
});

async function initTopics() {
    const admin = kafka.admin();
    
    try {
        console.log('Connecting to Kafka admin...');
        await admin.connect();
        
        const topics = [
            {
                topic: 'orders-events',
                numPartitions: 3,
                replicationFactor: 1,
            },
            {
                topic: 'payments-events',
                numPartitions: 3,
                replicationFactor: 1,
            },
            {
                // 1 partition — must match docker-compose kafka-init:
                // system-events is low volume and needs strict global ordering
                topic: 'system-events',
                numPartitions: 1,
                replicationFactor: 1,
            }
        ];

        console.log('Creating topics:', topics.map(t => t.topic).join(', '));
        
        const created = await admin.createTopics({
            validateOnly: false,
            waitForLeaders: true,
            topics: topics,
        }).catch(err => {
            // Ignore errors about topics already existing
            if (err.message && err.message.includes('Topic with this name already exists')) {
                return false;
            }
            throw err;
        });

        if (created) {
            console.log('Topics created successfully!');
        } else {
            console.log('Topics already exist.');
        }

    } catch (error) {
        console.error('Failed to create topics:', error.message);
    } finally {
        await admin.disconnect();
    }
}

initTopics();
