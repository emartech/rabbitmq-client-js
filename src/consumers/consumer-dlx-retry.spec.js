'use strict';

const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const Logger = require('@emartech/json-logger').Logger;

chai.use(sinonChai);

const expect = chai.expect;

const amqp = require('amqplib');
const RabbitMQConsumer = require('./consumer-dlx-retry');
const RabbitMQ = require('../rabbit-mq/rabbit-mq');
const RabbitMQSingleton = require('../rabbit-mq/index');
const RetryableError = require('../exceptions/retryable-error');

const channelName = 'test';
const loggerName = 'test';

const amqpConfig = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  }
};

describe.only('RabbitMQ Consumer with DLX Retry', function () {
  let sandbox = sinon.createSandbox();
  let clock;
  let startProcess;
  let ackStub;
  let nackStub;
  let prefetchStub;
  let cancelStub;

  beforeEach(async function () {
    clock = sandbox.useFakeTimers();
    startProcess = null;
    ackStub = sandbox.stub();
    nackStub = sandbox.stub();
    prefetchStub = sandbox.stub();
    cancelStub = sandbox.stub();

    const connectionMock = {
      createChannel: () =>
        Promise.resolve({
          assertQueue: () => {},
          on: () => {}
        })
    };
    sandbox.stub(amqp, 'connect').resolves(connectionMock);
    sandbox.stub(RabbitMQ.prototype, 'getChannel').returns({
      ack: ackStub,
      nack: nackStub,
      prefetch: prefetchStub,
      cancel: cancelStub,
      on: () => {},
      consume: async (channelName, consumeFn) => {
        startProcess = consumeFn;
        return Promise.resolve({ consumerTag: 'test-tag' });
      }
    });
  });

  afterEach(function () {
    sandbox.restore();
  });

  it('should create RabbitMqConsumer', async function () {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {}
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    expect(rabbitMQConsumer).is.instanceOf(RabbitMQConsumer);
  });

  it('should create a RabbitMQ connection with DLX options merged to config options', async function () {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      queueOptions: {
        foo: 'bar'
      },
      onMessage: async function () {}
    };

    const rabbitMqStub = sandbox.stub(RabbitMQSingleton, 'create');

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();

    expect(rabbitMqStub).have.been.calledWith(amqpConfig, channelName, 'default', {
      foo: 'bar',
      deadLetterExchange: '',
      deadLetterRoutingKey: `${channelName}-retry`
    });
  });

  it('should create a RabbitMQ connection for the DLX queue', async function () {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      retryTime: 300,
      onMessage: async function () {}
    };

    const rabbitMqStub = sandbox.stub(RabbitMQSingleton, 'create');

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();

    expect(rabbitMqStub).have.been.calledWith(amqpConfig, `${channelName}-retry`, 'default', {
      messageTtl: 300,
      deadLetterExchange: '',
      deadLetterRoutingKey: channelName
    });
  });

  it('should call onChannelEstablished with channel', async function () {
    const options = {
      logger: loggerName,
      channel: channelName,
      onChannelEstablished: sandbox.spy()
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, options);
    await rabbitMQConsumer.process();

    expect(options.onChannelEstablished).have.been.calledOnce;
  });

  it('should ack message if it is not parsable as JSON', async function () {
    const logFromErrorSpy = sandbox.spy(Logger.prototype, 'fromError');

    const message = { content: Buffer.from('Not a JSON') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {}
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    await startProcess(message);
    expect(ackStub).have.been.calledWith(message);
    expect(logFromErrorSpy).to.have.been.calledWith(
      'Consumer error finish',
      sinon.match.has('message', 'Unexpected token N in JSON at position 0'),
      { content: {} }
    );
  });

  it('should ack message if onMessage throws non-retryable error', async function () {
    const logFromErrorSpy = sandbox.spy(Logger.prototype, 'fromError');
    const message = { content: Buffer.from('{}') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {
        throw Error('test error');
      }
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    await startProcess(message);
    expect(ackStub).have.been.calledWith(message);
    expect(logFromErrorSpy).to.have.been.calledWith('Consumer error finish', sinon.match.has('message', 'test error'), {
      content: {}
    });
  });

  it('should nack message with requeue false when onMessage throws retryable error', async function () {
    const message = { content: Buffer.from('{ "a": "b" }') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {
        throw new RetryableError('test error');
      }
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    await startProcess(message);
    expect(nackStub).have.been.calledWith(message, false, false);
  });
});
