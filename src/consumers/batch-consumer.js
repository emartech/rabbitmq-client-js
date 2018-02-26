'use strict';

const ObjectBatcher = require('@emartech/object-batcher-js');
const RabbitMq = require('../rabbit-mq');
const { getLogger } = require('../util');

class RabbitMqBatchConsumer {
  constructor(amqpConfig, configuration) {
    this._logger = getLogger(configuration.logger);
    this._channel = configuration.channel;
    this._connectionType = configuration.connectionType || 'default';
    this._batchTimeout = configuration.batchTimeout || 1000;
    this._batchSize = configuration.batchSize || 1024;
    this._onMessages = configuration.onMessages;
    this._retryTime = configuration.retryTime || 60000;
    this._amqpConfig = amqpConfig;
    this._queueOptions = configuration.queueOptions || {};
    this._prefetchCount = configuration.prefetchCount || parseInt(process.env.PREFETCH_COUNT, 10) || 1024;
    this._objectBatcher = new ObjectBatcher(this._handleCollectedMessages.bind(this), {
      batchSize: this._batchSize,
      batchTimeout: this._batchTimeout
    });
    this._inProgress = 0;
    this._consumerCanceled = false;

    if (this._prefetchCount < this._batchSize) {
      throw new Error('Batch Consumer prefetchCount should be larger than batchSize');
    }
  }

  async process() {
    try {
      await this._setupRabbitMqChannel();
      this._consumer = await this._rabbitMqChannel.consume(this._channel, message => {
        this._inProgress++;
        const groupBy = message.properties.headers.groupBy;
        this._objectBatcher.add(groupBy, message);
      });
    } catch (error) {
      this._logger.fromError('Consumer initialization error', error);
    }
  }

  _handleCollectedMessages(groupBy, messageObjects) {
    let contents;
    try {
      contents = messageObjects.map(message => JSON.parse(message.content.toString()));
    } catch (error) {
      return this._consumerError(error, groupBy, messageObjects);
    }
    this._onMessages(groupBy, contents)
      .then(() => {
        return this._consumerSuccess(groupBy, messageObjects);
      })
      .catch(error => {
        if (error.retryable) {
          return this._consumerErrorWithDelayedRetry(error, groupBy, messageObjects);
        }
        return this._consumerError(error, groupBy, messageObjects);
      });
  }

  async _setupRabbitMqChannel() {
    if (!this._rabbitMqChannel) {
      this._rabbitMq = await RabbitMq.create(this._amqpConfig, this._channel, this._connectionType, this._queueOptions);
      this._rabbitMqChannel = this._rabbitMq.getChannel();
      await this._rabbitMqChannel.prefetch(this._prefetchCount);
    }
  }

  async _closeChannel() {
    if (this._inProgress) {
      return setTimeout(this._closeChannel, 100);
    }
    await this._rabbitMqChannel.close();
    await this._rabbitMq.closeConnection();
    process.exit(0);
  }

  async _closeAndCancelChannel() {
    if (this._consumerCanceled) {
      return;
    }
    this._consumerCanceled = true;
    await this._rabbitMqChannel.cancel(this._consumer.consumerTag);
    await this._closeChannel();
  }

  _consumerSuccess(groupBy, messageObjects) {
    this._logger.info('BatchConsumer-success', {
      group_by: groupBy,
      count: messageObjects.length
    });
    messageObjects.forEach(message => {
      this._rabbitMqChannel.ack(message);
    });
    this._inProgress -= messageObjects.length;
  }

  _consumerErrorWithDelayedRetry(error, groupBy, messageObjects) {
    this._logger.fromError('BatchConsumer error retry', error, {
      group_by: groupBy,
      count: messageObjects.length
    });
    return setTimeout(() => {
      messageObjects.forEach(message => this._rabbitMqChannel.nack(message));
      this._inProgress -= messageObjects.length;
    }, this._retryTime);
  }

  _consumerError(error, groupBy, messageObjects) {
    this._logger.fromError('BatchConsumer error finish', error, {
      error_data: error.data,
      group_by: groupBy,
      count: messageObjects.length
    });
    messageObjects.forEach(message => this._rabbitMqChannel.nack(message, false, false));
    this._inProgress -= messageObjects.length;
  }

  static create(amqpConfig, configuration) {
    return new RabbitMqBatchConsumer(amqpConfig, configuration);
  }
}

module.exports = RabbitMqBatchConsumer;
