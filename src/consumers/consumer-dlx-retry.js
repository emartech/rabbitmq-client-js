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
    this._queueOptions = {
      ...configuration.queueOptions,
      deadLetterExchange: '',
      deadLetterRoutingKey: `${configuration.channel}-retry-${this._retryTime}`
    };
    this._amqpConfig = amqpConfig;
  }

  async process() {
    const logger = this._logger;
    logger.info('[AMQP] Process');

    try {
      await RabbitMq.create(this._amqpConfig, `${this._channel}-retry-${this._retryTime}`, this._connectionType, {
        messageTtl: this._retryTime,
        deadLetterExchange: '',
        deadLetterRoutingKey: this._channel
      });

      const rabbitMq = await RabbitMq.create(this._amqpConfig, this._channel, this._connectionType, this._queueOptions);
      const channel = rabbitMq.getChannel();
      await channel.prefetch(this._prefetchCount);
      await this._onChannelEstablished(channel);

      channel.on('error', function (err) {
        logger.fromError('[AMQP] Channel error', err);
        throw new Error('[AMQP] Channel error');
      });

      channel.on('close', function () {
        logger.info('[AMQP] Channel close');
        process.exit(1);
      });

      const { consumerTag } = await channel.consume(this._channel, async (message) => {
        let content = {};

        try {
          content = JSON.parse(message.content.toString());
          await this._onMessage(content, message);
          await channel.ack(message);
        } catch (error) {
          if (error.retryable) {
            await channel.nack(message, false, false);
          } else {
            await channel.ack(message);
            if (this._loggerRules[error.message]) {
              logger.fromError('Consumer error finish', error, {
                content: _.pick(content, this._loggerRules[error.message])
              });
            } else {
              logger.fromError('Consumer error finish', error, { content });
            }
          }
        }
      });

      process.once('SIGTERM', () => {
        channel.cancel(consumerTag);
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
