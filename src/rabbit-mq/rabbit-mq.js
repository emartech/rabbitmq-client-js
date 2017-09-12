'use strict';

require('dotenv').config({ silent: true });

const url = require('url');
const amqp = require('amqplib');
const logger = require('logentries-logformat')('rabbit-mq-client');

class RabbitMq {
  constructor(amqpConfig, queueName, connectionType = 'default', queueOptions = {}) {
    this.queueName = queueName;
    this.queueOptions = queueOptions;
    this._amqpConfig = amqpConfig;
    this._connectionType = connectionType;
    this._connection = null;
    this._assertedQueues = {};
    this._channels = {};
  }

  async connect() {
    if (!this._connectionPromise) {
      const options = this._getOpts();
      this._connectionPromise = amqp.connect(this._config.url, options);
      this._connectionPromise.then(connection => {
        this._connection = connection;
      });
    }

    await this._connectionPromise;
  }

  _getOpts() {
    const parsedUrl = url.parse(this._config.url);

    return { servername: parsedUrl.hostname };
  }

  async createChannel() {
    this._validate();
    let registerCloseListener = false;

    if (!this._channels[this._connectionType]) {
      this._channels[this._connectionType] = this._connection.createChannel();
      registerCloseListener = true;
    }

    this._channel = await this._channels[this._connectionType];

    if (registerCloseListener) {
      this._channel.on('error', error => {
        logger.error('Channel error', error.message, JSON.stringify(error));
      });

      this._channel.on('close', () => {
        delete this._channels[this._connectionType];
        delete this._assertedQueues[this.queueName];
        logger.error('Channel close');
      });
    }

    await this._assertQueue();
  }

  async _assertQueue() {
    if (!this._assertedQueues[this.queueName]) {
      this._assertedQueues[this.queueName] = this._channel.assertQueue(this.queueName, this.queueOptions);
    }
    await this._assertedQueues[this.queueName];
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
    return this._amqpConfig;
  }
}

module.exports = RabbitMq;
