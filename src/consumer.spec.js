'use strict';

const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);

const expect = chai.expect;

const amqp = require('amqplib');
const RabbitMQConsumer = require('./consumer');
const RabbitMQ = require('./rabbit-mq');
const RabbitMQSingleton = require('../');
const RetryableError = require('./exceptions/retryable-error');

const channelName = 'test';
const loggerName = 'test';

const amqpConfig = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  }
};

describe('RabbitMQ Consumer', function() {
  let sandbox = sinon.sandbox.create();
  let clock;
  let startProcess;
  let ackStub;
  let nackStub;
  let prefetchStub;

  beforeEach(async function() {
    clock = sandbox.useFakeTimers();
    startProcess = null;
    ackStub = sandbox.stub();
    nackStub = sandbox.stub();
    prefetchStub = sandbox.stub();

    const connectionMock = {
      createChannel: () => Promise.resolve(true)
    };
    sandbox.stub(amqp, 'connect').resolves(connectionMock);
    sandbox.stub(RabbitMQ.prototype, 'getChannel').returns({
      ack: ackStub,
      nack: nackStub,
      prefetch: prefetchStub,
      on: () => {},
      consume: async (channelName, consumeFn) => { startProcess = consumeFn; return Promise.resolve(); }
    });
  });

  afterEach(function() {
    sandbox.restore();
  });

  it('should create RabbitMqConsumer', async function() {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function() {}
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);

    expect(rabbitMQConsumer).is.instanceOf(RabbitMQConsumer);
  });

  it('should create a RabbitMQ connection', async function() {
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function() {}
    };

    const rabbitMqStub = sandbox.stub(RabbitMQSingleton, 'create');

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();

    expect(rabbitMqStub).have.been.calledWith(amqpConfig, channelName);
  });

  it('should not retry when message is not parsable as JSON', async function() {
    const message = { content: new Buffer('Not a JSON') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function() {}
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should not retry when onMessage throws non-retryable error', async function() {
    const message = { content: new Buffer('{}') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function() { throw Error('test error'); }
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should retry when onMessage throws retryable error', async function() {
    const message = { content: new Buffer('{}') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      onMessage: async function() { throw new RetryableError('test error'); }
    };

    const rabbitMQConsumer = RabbitMQConsumer.create(amqpConfig, configuration);
    await rabbitMQConsumer.process();
    expect(typeof startProcess).to.be.eql('function');
    await startProcess(message);
    expect(nackStub).not.have.been.called;
    clock.tick(60000);
    expect(nackStub).have.been.calledWith(message);
  });

  it('should autonack if message processing takes too much time', async function() {
    const message = { content: new Buffer('{}') };
    const configuration = {
      logger: loggerName,
      channel: channelName,
      autoNackTime: 500,
      onMessage: async function() {
        await new Promise(resolve => setTimeout(resolve, 1000));
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
