'use strict';

require('dotenv').config({ silent: true });

const _ = require('lodash');
const url = require('url');
const amqp = require('amqplib');
const logger = require('@emartech/json-logger')('rabbit-mq-client');

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

  async createChannel(channels = {}, assertedQueues = {}, queueOptions = {}) {
    this._validate();
    let registerCloseListener = false;

    if (!channels[this._connectionType]) {
      channels[this._connectionType] = this._connection.createChannel();
      registerCloseListener = true;
    }

    this._channel = await channels[this._connectionType];

    if (registerCloseListener) {
      this._channel.on('error', error => {
        logger.fromError('Channel error', error);
      });

      this._channel.on('close', () => {
        _.unset(channels, [this._connectionType]);
        _.unset(assertedQueues, [[this._connectionType], [this.queueName]]);
        logger.warn('Channel close');
      });
    }

    await this._assertQueue(assertedQueues, queueOptions);
  }

  async _assertQueue(assertedQueues, queueOptions) {
    const defaultQueueOptions = { durable: false };
    const options = Object.assign({}, defaultQueueOptions, queueOptions);
    if (!_.get(assertedQueues, [[this._connectionType], [this.queueName]])) {
      _.set(
        assertedQueues,
        [[this._connectionType], [this.queueName]],
        this._channel.assertQueue(this.queueName, options)
      );
    }
    await assertedQueues[this._connectionType][this.queueName];
  }

  async closeConnection() {
    await this._connection.close();
  }

  async destroy() {
    await this._channel.deleteQueue(this.queueName);
  }

  insert(data, options = {}) {
    return this._channel.sendToQueue(this.queueName, new Buffer(JSON.stringify(data)), options);
  }

  insertWithGroupBy(groupBy, data, options = {}) {
    const insertOptions = Object.assign({}, options, { headers: { groupBy } });

    return this.insert(data, insertOptions);
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
