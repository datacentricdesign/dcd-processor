'use strict';

// Setting the logs
const log4js = require('log4js');
const logger = log4js.getLogger('[lib:kafka]');
logger.level = process.env.LOG_LEVEL || 'INFO';

const host = process.env.KAFKA_HOST || 'localhost';
const port = process.env.KAFKA_PORT !== undefined
    ? parseInt(process.env.KAFKA_PORT) : 9092;

const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const Offset = kafka.Offset;
const KafkaClient = kafka.KafkaClient;

/**
 * Connect to Kafka server
 **/
exports.connect = (topics, onMessage) => {
    logger.debug('Connecting to kafka...');
    const client = new KafkaClient({
        kafkaHost: host + ':' + port
    });

    const options = {
        groupId: 'kafka-node-influx',
        autoCommit: false,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024
    };

    const consumer = new Consumer(client, topics, options);
    const offset = new Offset(client);

    consumer.on('message', onMessage);

    consumer.on('error', (error) => {
        logger.error('error', error);
    });

    /*
    * If consumer get `offsetOutOfRange` event,
    * fetch data from the smallest(oldest) offset
    */
    consumer.on('offsetOutOfRange', (topic) => {
        topic.maxNum = 2;
        offset.fetch([topic], (error, offsets) => {
            if (error) {
                return logger.error(error);
            }
            const min = Math.min.apply(null,
                offsets[topic.topic][topic.partition]);
            consumer.setOffset(topic.topic, topic.partition, min);
        });
    });

};



pushData(topic, body, key) {
  logger.debug("pushData, key: " + key);
  if (this.enable) {
    if (this.producerIsReady) {
      let messages = [];
      let msgCount = 0;
      body.forEach(msg => {
        if (key !== undefined) {
          messages.push(new KeyedMessage(key, JSON.stringify(msg)));
        } else {
          messages.push(JSON.stringify(msg));
        }
        msgCount++;
        if (msgCount >= 1000) {
          logger.debug("Push 1000 messages to Kafka");
          this.sendToKafka(topic, messages);
          messages = [];
          msgCount = 0;
        }
      });
      if (messages.length > 0) {
        logger.debug("Push remaining messages to Kafka");
        return this.sendToKafka(topic, messages);
      }
    }
    return Promise.reject({
      code: 500,
      message: "Kafka producer not ready."
    });
  }
  return Promise.reject({ code: 500, message: "Kafka not enabled." });
}

/**
 * @param {String} topic
 * @param {String} messages
 * @returns {Promise}
 */
sendToKafka(topic, messages) {
  logger.debug(
    "Send Data to " + topic + " message: " + JSON.stringify(messages)
  );
  return new Promise((resolve, reject) => {
    const payloads = [{ topic: topic, messages: messages }];
    this.producer.send(payloads, error => {
      if (error) {
        return reject(error);
      }
      return resolve();
    });
  });
}