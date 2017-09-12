'use strict';

const Pool = require('./pool');

let channels = {};
let assertedQueues = {};

module.exports = {
  create: async (amqpConfig, queueName, connectionType) => {
    const rabbitMq = Pool.create(amqpConfig, connectionType).getClient(queueName);
    await rabbitMq.connect();
    await rabbitMq.createChannel(channels, assertedQueues);

    return rabbitMq;
  }
};
