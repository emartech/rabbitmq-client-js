'use strict';

const RabbitMq = require('./rabbit-mq');

const connections = {};

class RabbitMQPool {

  constructor(amqpConfig, connectionType = 'default', useGlobalPool = true) {
    this._amqpConfig = amqpConfig;
    this._connectionType = connectionType;
    this._connections = (useGlobalPool) ? connections : {};
  }

  getClient(queueName, queueOptions) {
    if (this._connectionExists() === false) {
      const amqpConfig = this._getAmqpConfig();
      const connection = new RabbitMq(amqpConfig, queueName, this._connectionType, queueOptions);
      this._saveConnection(connection);
    }

    return this._getConnection();
  }

  _saveConnection(connection) {
    this._connections[this._connectionType] = connection;
  }

  _getConnection() {
    return this._connections[this._connectionType];
  }

  _connectionExists() {
    return !!this._connections[this._connectionType];
  }

  _getAmqpConfig() {
    return this._amqpConfig[this._connectionType];
  }

  static create(amqpConfig, connectionType, useGlobalPool = true) {
    return new RabbitMQPool(amqpConfig, connectionType, useGlobalPool);
  }
}

module.exports = RabbitMQPool;
