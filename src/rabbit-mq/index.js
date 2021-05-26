'use strict';

const RabbitMq = require('./rabbit-mq');

let channels = {};
let connections = {};
let assertedQueues = {};

module.exports = {
  create: async (amqpConfig, queueName, connectionType, queueOptions, cryptoLib) => {
    const rabbitMq = new RabbitMq(amqpConfig, queueName, connectionType, cryptoLib);
    await rabbitMq.connect(connections);
    await rabbitMq.createChannel(channels, assertedQueues, queueOptions);

    return rabbitMq;
  },

  destroy: async () => {
    for (const connectionType in connections) {
      const connection = await connections[connectionType];
      await connection.close();
    }
  }
};
