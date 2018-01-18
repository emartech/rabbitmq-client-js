'use strict';

const _ = require('lodash');
const RabbitMq = require('../rabbit-mq/index');

class RabbitMqConsumer {

  constructor(amqpConfig, configuration) {
    this._logger = configuration.logger;
    this._channel = configuration.channel;
    this._connectionType = configuration.connectionType || 'default';
    this._onMessage = configuration.onMessage;
    this._retryTime = configuration.retryTime || 60000;
    this._prefetchCount = configuration.prefetchCount || parseInt(process.env.PREFETCH_COUNT, 10) || 1;
    this._loggerRules = configuration.loggerRules || {};
    this._autoNackTime = configuration.autoNackTime || false;
    this._queueOptions = configuration.queueOptions || {};
    this._amqpConfig = amqpConfig;
  }

  async process() {
    const logger = require('logentries-logformat')(this._logger);
    logger.log('[AMQP] Process');

    try {
      const rabbitMq = await RabbitMq.create(this._amqpConfig, this._channel, this._connectionType, this._queueOptions);
      const channel = rabbitMq.getChannel();
      await channel.prefetch(this._prefetchCount);

      channel.on('error', function(err) {
        logger.log('[AMQP] Channel error', { message: err.message });
        throw new Error('[AMQP] Channel error');
      });

      channel.on('close', function() {
        logger.log('[AMQP] Channel close');
        process.exit(1);
      });

      await channel.consume(this._channel, async (message) => {
        let autoNackTime;
        message._status = 'autonack init';
        if (typeof this._autoNackTime === 'number') {
          autoNackTime = setTimeout(() => {
            logger.error('Consumer auto nack', message._status, message.content.toString());
            channel.nack(message);
          }, this._autoNackTime);
        }

        let content = {};
        message._status = 'try';
        try {
          content = JSON.parse(message.content.toString());
          message._status = 'JSON parsed';
          await this._onMessage(content, message);
          message._status = 'after onMessage';
          if (autoNackTime) clearTimeout(autoNackTime);
          await channel.ack(message);
        } catch (error) {
          message._status = 'error';
          if (autoNackTime) clearTimeout(autoNackTime);

          if (error.retryable) {
            logger.error('Consumer error retry', error.message, content);
            return setTimeout(() => {
              channel.nack(message);
            }, this._retryTime);
          }

          channel.nack(message, false, false);
          if (this._loggerRules[error.message]) {
            logger.error('Consumer error finish', error.message, _.pick(content, this._loggerRules[error.message]));
          } else {
            logger.error('Consumer error finish', error.message, content);
          }
        }
      });
    } catch (error) {
      logger.error('Consumer initialization error', error.message, JSON.stringify({ error: error }));
    }
  };

  static create(amqpConfig, configuration) {
    return new RabbitMqConsumer(amqpConfig, configuration);
  }

}

module.exports = RabbitMqConsumer;
