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

const dummyCrypto = {
  async encrypt(str) {
    return str.split('').reverse().join('');
  },
  async decrypt(str) {
    return str.split('').reverse().join('');
  }
};

const amqpConfig = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  }
};

describe('RabbitMQ Batch Consumer', function () {
  let sandbox = sinon.createSandbox();
  let consume;
  let clock;
  let ackStub;
  let nackStub;
  let prefetchStub;

  beforeEach(async function () {
    consume = null;
    clock = sinon.useFakeTimers();
    ackStub = sandbox.stub();
    nackStub = sandbox.stub();
    prefetchStub = sandbox.stub();

    const connectionMock = {
      createChannel: () =>
        Promise.resolve({
          assertQueue: () => {}
        })
    };
    sandbox.stub(amqp, 'connect').resolves(connectionMock);
  });

  afterEach(function () {
    clock.restore();
    sandbox.restore();
  });

  it('should create a RabbitMQ connection', async function () {
    const rabbitMqSpy = sandbox.spy(RabbitMQSingleton, 'create');

    stubRabbitMq();
    await createConsumer();

    expect(rabbitMqSpy).have.been.calledWith(amqpConfig, channelName, 'default');
  });

  it('should create a RabbitMQ connection with the defined connectionType', async function () {
    const rabbitMqSpy = sandbox.spy(RabbitMQSingleton, 'create');

    stubRabbitMq();
    await createConsumer({
      connectionType: 'special'
    });

    expect(rabbitMqSpy).have.been.calledWith(amqpConfig, channelName, 'special');
  });

  it('should call onChannelEstablished with channel', async function () {
    stubRabbitMq();
    const options = {
      onChannelEstablished: sandbox.spy()
    };
    await createConsumer(options);

    expect(options.onChannelEstablished).have.been.calledOnce;
  });

  it('should call onMessages with batched messages', async function () {
    const message1 = createMessage({ content: '{"foo":"bar"}' });
    const message2 = createMessage({ content: '{"abc":"123"}' });

    let onMessagesArguments = null;
    stubRabbitMq();
    await createConsumer({
      onMessages: function () {
        onMessagesArguments = arguments;
        return Promise.resolve();
      }
    });

    await consume(message1);
    await consume(message2);

    expect(onMessagesArguments).to.be.null;

    clock.tick(60000);

    expect(onMessagesArguments[0]).to.be.eql('testGroup');
    expect(onMessagesArguments[1]).to.be.eql([{ foo: 'bar' }, { abc: '123' }]);
  });

  it('should call onMessages with decrypted batched messages', async function () {
    const encryptedMessageContent1 = await dummyCrypto.encrypt('{"foo":"bar"}');
    const encryptedMessageContent2 = await dummyCrypto.encrypt('{"abc":"123"}');

    const message1 = createMessage({ content: encryptedMessageContent1 });
    const message2 = createMessage({ content: encryptedMessageContent2 });

    let onMessagesArguments = null;
    stubRabbitMq();

    let resolveWait;
    const waitForOnMessages = new Promise((resolve) => {
      resolveWait = resolve;
    });

    await createConsumer({
      onMessages: async function () {
        onMessagesArguments = arguments;
        await resolveWait();
        return Promise.resolve();
      },
      cryptoLib: dummyCrypto
    });

    await consume(message1);
    await consume(message2);

    expect(onMessagesArguments).to.be.null;

    clock.tick(60000);

    await waitForOnMessages;

    expect(onMessagesArguments[0]).to.be.eql('testGroup');
    expect(onMessagesArguments[1]).to.be.eql([{ foo: 'bar' }, { abc: '123' }]);
  });

  it('should not retry when message is not parsable as JSON', async function () {
    const message = createMessage({ content: 'Not a JSON' });
    stubRabbitMq();
    await createConsumer({
      onMessages: async function () {}
    });

    await consume(message);

    clock.tick(60000);

    expect(nackStub).have.been.calledWith(message, false, false);
  });

  it('should not retry when message is not decryptable', function (done) {
    const encryptedMessageContent1 = '{"foo":"bar"}'.split('').reverse().join('');

    const message1 = createMessage({ content: encryptedMessageContent1 });

    nackStub = sinon.spy(function (message, allUpTo, requeue) {
      expect(message).to.eql(message1);
      expect(allUpTo).to.eql(false);
      expect(requeue).to.eql(false);
      done();
    });
    stubRabbitMq();

    createConsumer({
      onMessages: async function () {
        return Promise.resolve();
      },
      cryptoLib: {
        async decrypt() {
          throw new Error('Boom!');
        }
      }
    }).then(() => {
      consume(message1);
      clock.tick(60000);
    });
  });

  it('should not retry when message is decryptable but not parsable', function (done) {
    const encryptedMessageContent1 = 'Not a json'.split('').reverse().join('');

    const message1 = createMessage({ content: encryptedMessageContent1 });

    nackStub = sinon.spy(function (message, allUpTo, requeue) {
      expect(message).to.eql(message1);
      expect(allUpTo).to.eql(false);
      expect(requeue).to.eql(false);
      done();
    });
    stubRabbitMq();

    createConsumer({
      onMessages: async function () {
        return Promise.resolve();
      },
      cryptoLib: dummyCrypto
    }).then(() => {
      consume(message1);
      clock.tick(60000);
    });
  });

  it('should not retry when onMessage throws non-retryable error', function (done) {
    const message = createMessage({ content: '{}' });
    nackStub = sinon.spy(function (message, allUpTo, requeue) {
      expect(message).to.eql(message);
      expect(allUpTo).to.false;
      expect(requeue).to.false;
      done();
    });
    stubRabbitMq();
    createConsumer({
      onMessages: async function () {
        throw new Error('test error');
      }
    }).then(() => {
      consume(message);
      clock.tick(60000);
    });
  });

  it('should retry when onMessage throws retryable error', function (done) {
    clock.restore();
    const message = createMessage({ content: '{}' });
    nackStub = sinon.spy(function (message, allUpTo, requeue) {
      expect(message).to.eql(message);
      expect(allUpTo).to.undefined;
      expect(requeue).to.undefined;
      done();
    });

    stubRabbitMq();
    createConsumer({
      batchTimeout: 1,
      retryTime: 1,
      onMessages: async function () {
        throw new RetryableError('test error');
      }
    }).then(() => {
      consume(message);
    });
  });

  const stubRabbitMq = function () {
    sandbox.stub(RabbitMQ.prototype, 'getChannel').returns({
      ack: ackStub,
      nack: nackStub,
      prefetch: prefetchStub,
      consume: async (channelName, consumeFn) => {
        consume = consumeFn;
        return Promise.resolve();
      }
    });
  };

  const createConsumer = function (options = {}) {
    const configuration = Object.assign(
      {
        logger: loggerName,
        channel: channelName,
        cancel: () => {},
        onMessages: async function () {}
      },
      options
    );
    const rabbitMqBatchConsumer = RabbitMQBatchConsumer.create(amqpConfig, configuration);
    return rabbitMqBatchConsumer.process();
  };

  const createMessage = function (options = {}) {
    const content = Buffer.from(options.content || {});
    const properties = options.properties || { headers: { groupBy: 'testGroup' } };
    return { content, properties };
  };
});
