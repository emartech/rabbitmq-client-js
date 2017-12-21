'use strict';

const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);

const expect = chai.expect;

const amqp = require('amqplib');
const RabbitMQBatchConsumer = require('./batch-consumer');
const RabbitMQSingleton = require('../rabbit-mq/index');
const RabbitMQ = require('../rabbit-mq/rabbit-mq');
const RetryableError = require('../exceptions/retryable-error');

const channelName = 'test';
const loggerName = 'test';


const amqpConfig = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  },
  special: {
    url: 'amqp://spec:ial@192.168.40.10:5672/special'
  }
};

describe('RabbitMQ Batch Consumer', function() {
  let sandbox = sinon.sandbox.create();
  let consume;
  let clock;
  let ackStub;
  let nackStub;
  let prefetchStub;
  let cancelStub;

  beforeEach(async function() {
    consume = null;
    clock = sinon.useFakeTimers();
    ackStub = sandbox.stub();
    nackStub = sandbox.stub();
    prefetchStub = sandbox.stub();
    cancelStub = sandbox.stub();

    const connectionMock = {
      createChannel: () => Promise.resolve({
        assertQueue: () => {},
        on: () => {}
      })
    };
    sandbox.stub(amqp, 'connect').resolves(connectionMock);

  });

  afterEach(function() {
    clock.restore();
    sandbox.restore();
  });

  it('should create a RabbitMQ connection', async function() {
    const rabbitMqSpy = sandbox.spy(RabbitMQSingleton, 'create');

    stubRabbitMq();
    await createConsumer();

    expect(rabbitMqSpy).have.been.calledWith(amqpConfig, channelName, 'default');
  });

  it('should create a RabbitMQ connection with the defined connectionType', async function() {
    const rabbitMqSpy = sandbox.spy(RabbitMQSingleton, 'create');

    stubRabbitMq();
    await createConsumer({
      connectionType: 'special'
    });

    expect(rabbitMqSpy).have.been.calledWith(amqpConfig, channelName, 'special');
  });

  it('should create a RabbitMQ connection with the defined queueOptions', async function() {
    const rabbitMqSpy = sandbox.spy(RabbitMQSingleton, 'create');

    stubRabbitMq();
    await createConsumer({
      connectionType: 'special',
      queueOptions: { durable: true }
    });

    expect(rabbitMqSpy).have.been.calledWith(amqpConfig, channelName, 'special', { durable: true });
  });

  it('should call onMessages with batched messages', async function() {
    const message1 = createMessage({ content: '{"foo":"bar"}' });
    const message2 = createMessage({ content: '{"abc":"123"}' });

    let onMessagesArguments = null;
    stubRabbitMq();
    const consumer = await createConsumer({
      onMessages: async function() { onMessagesArguments = arguments; }
    });

    expect(consumer.isFinished()).to.eql(true);

    await consume(message1);
    await consume(message2);

    expect(onMessagesArguments).to.be.null;
    expect(consumer.isFinished()).to.eql(false);

    clock.tick(60000);

    expect(onMessagesArguments[0]).to.be.eql('testGroup');
    expect(onMessagesArguments[1]).to.be.eql([{ foo: 'bar' }, { abc: '123' }]);
    await waitForNextTick();
    expect(consumer.isFinished()).to.eql(true);
  });

  it('should not retry when message is not parsable as JSON', async function() {
    const message = createMessage({ content: 'Not a JSON' });
    stubRabbitMq();
    await createConsumer({
      onMessages: async function() {}
    });

    await consume(message);

    clock.tick(60000);

    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should not retry when onMessage throws non-retryable error', function(done) {
    const message = createMessage({ content: '{}' });
    nackStub = sinon.spy(function(message, allUpTo, requeue) {
      expect(message).to.eql(message);
      expect(allUpTo).to.false;
      expect(requeue).to.false;
      done();
    });
    stubRabbitMq();
    createConsumer({
      onMessages: async function() { throw new Error('test error'); }
    }).then(() => {
      consume(message);
      clock.tick(60000);
    });

  });


  it('should retry when onMessage throws retryable error', function(done) {
    clock.restore();
    const message = createMessage({ content: '{}' });
    nackStub = sinon.spy(function(message, allUpTo, requeue) {
      expect(message).to.eql(message);
      expect(allUpTo).to.undefined;
      expect(requeue).to.undefined;
      done();
    });
    stubRabbitMq();
    createConsumer({
      batchTimeout: 1,
      retryTime: 1,
      onMessages: async function() { throw new RetryableError('test error'); }
    }).then(() => {
      consume(message);
    });

  });

  describe('#stopConsumption', function() {
    it('should cancel the consumer', async function() {
      stubRabbitMq();
      const configuration = getTestConfiguration();
      const rabbitMqBatchConsumer = RabbitMQBatchConsumer.create(amqpConfig, configuration);
      await rabbitMqBatchConsumer.process();
      const result = await rabbitMqBatchConsumer.stopConsumption();
      expect(cancelStub).to.have.been.calledWith('testTag');
      expect(result).to.eql(true);
    });

    it('should return false if there is nothing to stop', async function() {
      stubRabbitMq();
      const rabbitMqBatchConsumer = await createConsumer();
      await rabbitMqBatchConsumer.stopConsumption();
      const result = await rabbitMqBatchConsumer.stopConsumption();
      expect(result).to.eql(false);
    });

    it('should be able to restart the consumption', async function() {
      stubRabbitMq();
      const onMessageStub = this.sandbox.stub().returns(Promise.resolve());
      const testMessage = createMessage({ content: '{"foo":"bar"}' });

      const rabbitMqBatchConsumer = await createConsumer({
        onMessages: onMessageStub
      });

      const firstConsumer = consume;
      await consume(testMessage);
      clock.tick(60000);

      await rabbitMqBatchConsumer.stopConsumption();
      await rabbitMqBatchConsumer.process();

      await consume(testMessage);
      clock.tick(60000);

      await rabbitMqBatchConsumer.stopConsumption();

      expect(cancelStub).to.have.been.calledTwice;
      expect(onMessageStub).to.have.been.calledTwice;
      expect(firstConsumer).to.not.eq(consume);
    });
  });

  const stubRabbitMq = function() {
    sandbox.stub(RabbitMQ.prototype, 'getChannel').returns({
      ack: ackStub,
      nack: nackStub,
      prefetch: prefetchStub,
      cancel: cancelStub,
      consume: async (channelName, consumeFn) => { consume = consumeFn; return { consumerTag: 'testTag' }; }
    });
  };

  const createConsumer = async function(options = {}) {
    const configuration = getTestConfiguration(options);
    const rabbitMqBatchConsumer = RabbitMQBatchConsumer.create(amqpConfig, configuration);
    await rabbitMqBatchConsumer.process();
    return rabbitMqBatchConsumer;
  };

  const getTestConfiguration = function(options = {}) {
    return Object.assign({
      logger: loggerName,
      channel: channelName,
      onMessages: async function() {}
    }, options);
  };

  const createMessage = function(options = {}) {
    const content = new Buffer(options.content || {});
    const properties = options.properties || { headers: { groupBy: 'testGroup' } };
    return { content, properties };
  };

  const waitForNextTick = function() {
    return new Promise(resolve => process.nextTick(resolve));
  }
});

