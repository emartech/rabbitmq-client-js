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
    this._queueAsserted = false;
    this._channel = null;
  }

  async connect() {
    if (!this._connectionProgress) {
      const options = this._getOpts();
      this._connectionProgress = amqp.connect(this._config.url, options);
      this._connectionProgress.then(connection => {
        this._connection = connection;
      });
    }

    await this._connectionProgress;
  }

  _getOpts() {
    const parsedUrl = url.parse(this._config.url);

    return { servername: parsedUrl.hostname };
  }

  async createChannel() {
    this._validate();

    if (!this._channelProgress) {
      this._channelProgress = this._connection.createChannel();
      this._channel = await this._channelProgress;

      this._channel.on('error', error => {
        logger.error('Channel error', error.message, JSON.stringify(error));
      });

      this._channel.on('close', () => {
        this._channel = null;
        this._channelProgress = null;
        this._queueAsserted = false;
        logger.error('Channel close');
      });

      await this._assertQueue();
    }
  }

  async _assertQueue() {
    if (!this._queueAsserted) {
      this._queueAsserted = true;
    }

    await this._channel.assertQueue(this.queueName, this.queueOptions);
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
