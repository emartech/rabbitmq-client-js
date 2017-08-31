'use strict';

const RabbitMq = require('./rabbit-mq');

const connections = {};

class RabbitMQPool {

  constructor(amqpConfig, connectionType = 'default', pool = {}) {
    this._amqpConfig = amqpConfig;
    this._connectionType = connectionType;
    this._connectionPool = pool;
  }

  getClient(queueName, queueOptions) {
    if (this._connectionExists() === false) {
      const amqpConfig = this._getAmqpConfig();
      const connection = new RabbitMq(amqpConfig, queueName, queueOptions);
      this._saveConnection(connection);
    }

    return this._getConnection();
  }

  _saveConnection(connection) {
    this._connectionPool[this._connectionType] = connection;
  }

  _getConnection() {
    return this._connectionPool[this._connectionType];
  }

  _connectionExists() {
    return !!this._connectionPool[this._connectionType];
  }

  _getAmqpConfig() {
    return this._amqpConfig[this._connectionType];
  }

  static create(amqpConfig, connectionType) {
    return new RabbitMQPool(amqpConfig, connectionType, connections);
  }

  static createFromNewPool(amqpConfig, connectionType) {
    return new RabbitMQPool(amqpConfig, connectionType, {});
  }
}

module.exports = RabbitMQPool;
