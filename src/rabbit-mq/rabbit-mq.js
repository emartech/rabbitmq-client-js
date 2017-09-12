'use strict';

require('dotenv').config({ silent: true });

const url = require('url');
const amqp = require('amqplib');
const logger = require('logentries-logformat')('rabbit-mq-client');

class RabbitMq {
  constructor(amqpConfig, queueName, connectionType = 'default',  queueOptions = {}) {
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

  async createChannel(channels = {}, assertedQueues = {}) {
    this._validate();
    let registerCloseListener = false;

    if (!channels[this._connectionType]) {
      channels[this._connectionType] = this._connection.createChannel();
      registerCloseListener = true;
    }

    this._channel = await channels[this._connectionType];


    if (registerCloseListener) {
      this._channel.on('error', error => {
        logger.error('Channel error', error.message, JSON.stringify(error));
      });

      this._channel.on('close', () => {
        delete channels[this._connectionType];
        delete assertedQueues[this.queueName];
        logger.error('Channel close');
      });
    }

    await this._assertQueue(assertedQueues);
  }

  async _assertQueue(assertedQueues) {
    if (!assertedQueues[this.queueName]) {
      assertedQueues[this.queueName] = this._channel.assertQueue(this.queueName, { durable: false });
    }
    await assertedQueues[this.queueName];
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
