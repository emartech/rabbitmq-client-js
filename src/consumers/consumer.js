'use strict';

const _ = require('lodash');
const RabbitMq = require('../rabbit-mq/index');
const { getLogger } = require('../util');

class RabbitMqConsumer {
  constructor(amqpConfig, configuration) {
    this._logger = getLogger(configuration.logger);
    this._channel = configuration.channel;
    this._connectionType = configuration.connectionType || 'default';
    this._onChannelEstablished = configuration.onChannelEstablished || (async () => Promise.resolve());
    this._onMessage = configuration.onMessage;
    this._retryTime = configuration.retryTime || 60000;
    this._prefetchCount = configuration.prefetchCount || parseInt(process.env.PREFETCH_COUNT, 10) || 1;
    this._loggerRules = configuration.loggerRules || {};
    this._autoNackTime = configuration.autoNackTime || false;
    this._queueOptions = configuration.queueOptions || {};
    this._amqpConfig = amqpConfig;
    this._logRetriableErrorContent = configuration.logRetriableErrorContent || false;
  }

  async process() {
    const logger = this._logger;
    logger.info('[AMQP] Process');

    try {
      const rabbitMq = await RabbitMq.create(this._amqpConfig, this._channel, this._connectionType, this._queueOptions);
      const channel = rabbitMq.getChannel();
      await channel.prefetch(this._prefetchCount);
      await this._onChannelEstablished(channel);

      channel.on('error', function(err) {
        logger.fromError('[AMQP] Channel error', err);
        throw new Error('[AMQP] Channel error');
      });

      channel.on('close', function() {
        logger.info('[AMQP] Channel close');
        process.exit(1);
      });

      await channel.consume(this._channel, async message => {
        let autoNackTime;
        if (typeof this._autoNackTime === 'number') {
          autoNackTime = setTimeout(() => {
            logger.error('Consumer auto nack', { content: message.content.toString() });
            channel.nack(message);
          }, this._autoNackTime);
        }

        let content = {};

        try {
          content = JSON.parse(message.content.toString());
          await this._onMessage(content, message);
          if (autoNackTime) clearTimeout(autoNackTime);
          await channel.ack(message);
        } catch (error) {
          if (autoNackTime) clearTimeout(autoNackTime);

          if (error.retryable) {
            logger.fromError('Consumer error retry', error, this._logRetriableErrorContent ? { content } : {});
            return setTimeout(() => {
              channel.nack(message);
            }, this._retryTime);
          }

          channel.nack(message, false, false);
          if (this._loggerRules[error.message]) {
            logger.fromError('Consumer error finish', error, {
              content: _.pick(content, this._loggerRules[error.message])
            });
          } else {
            logger.fromError('Consumer error finish', error, { content });
          }
        }
      });
    } catch (error) {
      logger.fromError('Consumer initialization error', error);
    }
  }

  static create(amqpConfig, configuration) {
    return new RabbitMqConsumer(amqpConfig, configuration);
  }
}

module.exports = RabbitMqConsumer;
