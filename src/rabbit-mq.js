'use strict';

require('dotenv').config({ silent: true });

const url = require('url');
const amqp = require('amqplib');

class RabbitMq {
  constructor(amqpConfig, queueName, connectionType = 'default') {
    this.queueName = queueName;
    this._amqpConfig = amqpConfig;
    this._connectionType = connectionType;
    this._connection = null;
  }

  async connect(connections = {}) {
    if (connections[this._connectionType]) {
      this._connection = await connections[this._connectionType];
      return;
    }

    const opts = this._getOpts();
    connections[this._connectionType] = amqp.connect(this._config.url, opts);

    this._connection = await connections[this._connectionType];
  }

  _getOpts() {
    const parsedUrl = url.parse(this._config.url);

    return { servername: parsedUrl.hostname };
  }

  async createChannel(channels = {}) {
    this._validate();

    if (!channels[this._connectionType]) {
      channels[this._connectionType] = this._connection.createChannel();
    }

    this._channel = await channels[this._connectionType];
  }

  async closeConnection() {
    await this._connection.close();
  }

  async destroy() {
    await this._channel.deleteQueue(this.queueName);
  }

  insert(data) {
    return this._channel.sendToQueue(this.queueName, new Buffer(JSON.stringify(data)));
  }

  insertWithGroupBy(groupBy, data) {
    return this._channel.sendToQueue(
      this.queueName,
      new Buffer(JSON.stringify(data)),
      { headers: { groupBy } }
    );
  }

  async purge() {
    await this._channel.purgeQueue(this.queueName);
  }

  getChannel() {
    return this._channel;
  }

  _validate() {
    if (!this._connection) {
      throw Error('No RabbitMQ connection');
    }

    if (!this.queueName) {
      throw Error('No RabbitMQ queue');
    }
  }

  get _config() {
    return this._amqpConfig[this._connectionType];
  }
}

module.exports = RabbitMq;
