const kafka = require('kafka-node');
const uuid = require('uuid');

const client = new kafka.Client('velomobile.srvs.cloudkafka.com', 'my-client-id', {
    sessionTimeout: 300,
    spinDelay: 100,
    retries: 2
});

const producer = new kafka.HighLevelProducer(client);
producer.on('ready', function() {
    console.log('Kafka Producer is connected and ready.');
});

// For this demo we just log producer errors to the console.
producer.on('error', function(error) {
    console.error(error);
});

const KafkaService = {
    sendRecord: ({ type, userId, sessionId, data }, callback = () => {}) => {
        if (!userId) {
            return callback(new Error(`A userId must be provided.`));
        }

        const event = {
            id: uuid.v4(),
            timestamp: Date.now(),
            userId,
            sessionId,
            type,
            data
        };

        const buffer = new Buffer.from(JSON.stringify(event));

        // Create a new payload
        const record = [{
            topic: 'aj9g740d-default',
            messages: buffer,
            attributes: 1 /* Use GZip compression for the payload */
        }];

        //Send record to Kafka and log result/error
        producer.send(record, callback);
    }
};

module.exports = KafkaService;
