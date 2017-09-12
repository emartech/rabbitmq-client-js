'use strict';

const RabbitMq = require('./rabbit-mq');

let channels = {};
let connections = {};
let assertedQueues = {};

module.exports = {
  create: async (amqpConfig, queueName, connectionType) => {
    const rabbitMq = new RabbitMq(amqpConfig, queueName, connectionType);
    await rabbitMq.connect(connections);
    await rabbitMq.createChannel(channels, assertedQueues);

    return rabbitMq;
  }
};
