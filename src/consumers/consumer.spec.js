'use strict';

const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const Logger = require('@emartech/json-logger').Logger;

chai.use(sinonChai);

const expect = chai.expect;

const amqp = require('amqplib');
const RabbitMQConsumer = require('./consumer');
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

const dummyCrypto = {
  async encrypt(str) {
    return str.split('').reverse().join('');
  },
  async decrypt(str) {
    return str.split('').reverse().join('');
  }
};

describe('RabbitMQ Consumer', function () {
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

  it('should create a RabbitMQ connection', async function () {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {}
    };

    const rabbitMqStub = sandbox.stub(RabbitMQSingleton, 'create');

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();

    expect(rabbitMqStub).have.been.calledWith(amqpConfig, channelName);
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

  it('should call onMessage with parsed content', async function () {
    const message = { content: Buffer.from('{"alma":"fa"}') };
    const onMessageSpy = sandbox.spy();
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: onMessageSpy
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    await startProcess(message);

    expect(onMessageSpy).to.have.been.calledWith({ alma: 'fa' });
  });

  it('should call onMessage with decrypted and parsed content in crypto is set', async function () {
    const encyptedContent = await dummyCrypto.encrypt(JSON.stringify({ alma: 'fa' }));
    const message = { content: Buffer.from(encyptedContent) };
    const onMessageSpy = sandbox.spy();
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: onMessageSpy,
      cryptoLib: dummyCrypto
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    await startProcess(message);

    expect(onMessageSpy).to.have.been.calledWith({ alma: 'fa' });
  });

  it('should not retry when message is not parsable as JSON', async function () {
    const message = { content: Buffer.from('Not a JSON') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {}
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should not retry when onMessage throws non-retryable error', async function () {
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
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should retry when onMessage throws retryable error', async function () {
    const logFromErrorSpy = sandbox.spy(Logger.prototype, 'fromError');

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
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(logFromErrorSpy).to.have.been.calledWith('Consumer error retry', sinon.match.any, {});

    expect(nackStub).not.have.been.called;
    clock.tick(60000);
    expect(nackStub).have.been.calledWith(message);
  });

  it('should log retriable error content if configured', async function () {
    const logFromErrorSpy = sandbox.spy(Logger.prototype, 'fromError');

    const message = { content: Buffer.from('{ "a": "b" }') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function () {
        throw new RetryableError('test error');
      },
      logRetriableErrorContent: true
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(logFromErrorSpy).to.have.been.calledWith('Consumer error retry', sinon.match.any, { content: { a: 'b' } });

    expect(nackStub).not.have.been.called;
    clock.tick(60000);
    expect(nackStub).have.been.calledWith(message);
  });

  it('should autonack if message processing takes too much time', async function () {
    const message = { content: Buffer.from('{}') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      autoNackTime: 500,
      onMessage: async function () {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    startProcess(message);
    expect(nackStub).not.have.been.called;
    clock.tick(501);
    expect(nackStub).have.been.calledWith(message);
  });
});
