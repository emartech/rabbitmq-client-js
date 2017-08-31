'use strict';

const Pool = require('./pool');

module.exports = {
  create: async (amqpConfig, queueName, connectionType, queueOptions) => {
    const rabbitMq = Pool.create(amqpConfig, connectionType).getClient(queueName, queueOptions);
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    return rabbitMq;
  }
};
