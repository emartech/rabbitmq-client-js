'use strict';

require('dotenv').config({ silent: true });

const _ = require('lodash');
const url = require('url');
const amqp = require('amqplib');
const logger = require('@emartech/json-logger')('rabbit-mq-client');

class RabbitMq {
  constructor(amqpConfig, queueName, connectionType = 'default', cryptoLib) {
    this.queueName = queueName;
    this._amqpConfig = amqpConfig;
    this._connectionType = connectionType;
    this._connection = null;
    this._cryptoLib = cryptoLib;
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
      channels[this._connectionType] = this._createChannel();
      registerCloseListener = true;
    }

    this._channel = await channels[this._connectionType];

    if (registerCloseListener) {
      this._channel.on('error', (error) => {
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

  _createChannel() {
    if (this._config.useConfirmChannel) {
      return this._connection.createConfirmChannel();
    }

    return this._connection.createChannel();
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

  async insert(data, options = {}) {
    let payload;

    if (this._cryptoLib) {
      payload = await this._cryptoLib.encrypt(JSON.stringify(data));
    } else {
      payload = JSON.stringify(data);
    }

    return this._channel.sendToQueue(this.queueName, Buffer.from(payload), options);
  }

  insertWithGroupBy(groupBy, data, options = {}) {
    const insertOptions = Object.assign({}, options, { headers: { groupBy } });

    return this.insert(data, insertOptions);
  }

  async waitForConfirms() {
    if (this._config.useConfirmChannel) {
      return await this._channel.waitForConfirms();
    }

    throw new Error('Waiting for confirmation is only supported with confirmation channels');
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
