'use strict';

const amqp = require('amqplib');
const RabbitMq = require('./rabbit-mq');
const chai = require('chai');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');
const EventEmitter = require('events');


chai.use(sinonChai);
chai.use(chaiAsPromised);

const expect = chai.expect;

const config = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/fakeDefault'
  },
  lobab: {
    url: 'amqp://test:secret@192.168.40.254:5672/fakeLobab'
  }
};
const queueName = 'test-queue';
const connectionType = 'default';

describe('RabbitMQ', () => {
  let rabbitMq;
  let sandbox = sinon.sandbox.create();

  let connectionMock;
  let channelMock;

  beforeEach(async () => {
    channelMock = Object.assign(new EventEmitter(), {
      sendToQueue: sandbox.stub().returns(true),
      deleteQueue: sandbox.stub().resolves(true),
      purgeQueue: sandbox.stub().resolves(true),
      assertQueue: sandbox.stub().resolves(true)
    });

    connectionMock = {
      createChannel: sandbox.stub().resolves(channelMock),
      close: sandbox.stub().returns(true)
    };

    sandbox.stub(amqp, 'connect').resolves(connectionMock);
    rabbitMq = new RabbitMq(config, queueName);
  });

  afterEach(async () => {
    sandbox.restore();
  });

  it('#connect should call amqp connect with the right parameters on default connection', async () => {
    await rabbitMq.connect();
    expect(amqp.connect).to.have.been.calledWith(
      'amqp://test:secret@192.168.40.10:5672/fakeDefault',
      { servername: '192.168.40.10' }
    );
  });

  it('#connect should call amqp connect with the right parameters on no-default connection', async () => {
    rabbitMq = new RabbitMq(config, queueName, 'lobab');

    await rabbitMq.connect();
    expect(amqp.connect).to.have.been.calledWith(
      'amqp://test:secret@192.168.40.254:5672/fakeLobab',
      { servername: '192.168.40.254' }
    );
  });

  it('#connect cache the connection', async () => {
    const connections = {};
    await rabbitMq.connect(connections);
    const connection = await connections.default;

    expect(connection).to.be.equal(connectionMock);
  });

  it('#connect should reuse existing connection if it was already created', async () => {
    const localConnectionMock = {
      close: sandbox.stub().resolves(true)
    };
    const connections = { default: Promise.resolve(localConnectionMock) };
    await rabbitMq.connect(connections);

    await rabbitMq.closeConnection();
    expect(localConnectionMock.close).to.have.been.calledOnce;
  });

  it('#createChannel should check if connection is ready', async () => {
    await expect(rabbitMq.createChannel()).to.be.rejectedWith('No RabbitMQ connection');
    await rabbitMq.connect();
    await expect(rabbitMq.createChannel()).to.be.fulfilled;
  });

  it('#createChannel should cache the channel and assert the queue with default queue options', async () => {
    const assertQueueValue = { testing: 123 };
    channelMock.assertQueue = sandbox.stub().resolves(assertQueueValue);

    const channels = {};
    const assertedQueues = {};
    await rabbitMq.connect();
    await rabbitMq.createChannel(channels, assertedQueues);

    const channel = await channels.default;

    expect(channel).to.be.equal(channelMock);
    expect(channelMock.assertQueue).to.have.been.calledWith(queueName, { durable: false });
    expect(await assertedQueues[connectionType][queueName]).to.eql(assertQueueValue);
  });

  it('#createChannel should cache the channel and assert the queue', async () => {
    const assertQueueValue = { testing: 123 };
    channelMock.assertQueue = sandbox.stub().resolves(assertQueueValue);

    const channels = {};
    const assertedQueues = {};
    const queueOptions = { durable: true };
    await rabbitMq.connect();
    await rabbitMq.createChannel(channels, assertedQueues, queueOptions);

    expect(channelMock.assertQueue).to.have.been.calledWith(queueName, { durable: true });
  });

  it('#createChannel should reuse existing channel and assertQueue if it was already created', async () => {
    const localChannelMock = Object.assign({}, channelMock);
    const channels = { default: Promise.resolve(localChannelMock) };

    const assertedQueues = {
      default: {}
    };
    assertedQueues[connectionType][queueName] = 'called';

    await rabbitMq.connect();
    await rabbitMq.createChannel(channels, assertedQueues);

    expect(await rabbitMq.getChannel()).to.be.eql(localChannelMock);
    expect(localChannelMock.assertQueue).not.to.have.been.called;
  });

  it('#createChannel create separate channels and assertQueues if called with different connectionType', async () => {
    const assertQueueValueDefault = { fake: 123 };
    const assertQueueValueLobab = { fakeLobab: 123 };

    channelMock.assertQueue = sandbox.stub()
      .onCall(0).resolves(assertQueueValueDefault)
      .onCall(1).resolves(assertQueueValueLobab);

    const channels = {};
    const assertedQueues = {};

    await rabbitMq.connect();
    await rabbitMq.createChannel(channels, assertedQueues);

    const rabbitMq2 = new RabbitMq(config, queueName, 'lobab');
    await rabbitMq2.connect();
    await rabbitMq2.createChannel(channels, assertedQueues);

    expect(channelMock.assertQueue).to.have.been.calledTwice;
    expect(await assertedQueues.default[queueName]).to.eql(assertQueueValueDefault);
    expect(await assertedQueues.lobab[queueName]).to.eql(assertQueueValueLobab);
  });

  it('#createChannel should check if queueName was set', async () => {
    rabbitMq = new RabbitMq(config);
    await rabbitMq.connect();
    await expect(rabbitMq.createChannel()).to.be.rejectedWith('No RabbitMQ queue');
  });

  it('#insert should call sentToQueue', async () => {
    const data = { test: 'data' };
    await rabbitMq.connect();
    await rabbitMq.createChannel();
    rabbitMq.insert(data);
    expect(channelMock.sendToQueue).to.have.been.calledWith(queueName, new Buffer(JSON.stringify(data)));
  });

  it('#insert should support options parameter', async () => {
    const data = { test: 'data' };
    const options = { test: 'ing' };
    await rabbitMq.connect();
    await rabbitMq.createChannel();
    rabbitMq.insert(data, options);
    expect(channelMock.sendToQueue).to.have.been.calledWith(queueName, new Buffer(JSON.stringify(data)), options);
  });

  it('#insertWithGroupBy should call sentToQueue', async () => {
    const groupBy = 'me.login';
    const data = { test: 'data' };
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    rabbitMq.insertWithGroupBy(groupBy, data);
    expect(channelMock.sendToQueue).to.have.been.calledWith(
      queueName,
      new Buffer(JSON.stringify(data)),
      { headers: { groupBy } }
    );
  });

  it('#insertWithGroupBy should support options parameter', async () => {
    const groupBy = 'me.login';
    const data = { test: 'data' };
    const options = { test: 'ing' };
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    rabbitMq.insertWithGroupBy(groupBy, data, options);
    expect(channelMock.sendToQueue).to.have.been.calledWith(
      queueName,
      new Buffer(JSON.stringify(data)),
      Object.assign({ headers: { groupBy } }, options)
    );
  });

  it('#purge should empty the queue', async () => {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.purge();

    expect(channelMock.purgeQueue).to.have.been.calledWith(queueName);
  });

  it('#closeConnection should close the rabbitMq connection', async () => {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.closeConnection();
    expect(connectionMock.close).to.have.been.calledOnce;
  });

  it('#destroy should delete the queue', async () => {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.destroy();
    expect(channelMock.deleteQueue).to.have.been.calledWith(queueName);
  });

  describe('with dead channel', () => {

    it('should remove channel from the cache', async () => {
      const channels = {};
      const assertedQueues = {};

      await rabbitMq.connect();
      await rabbitMq.createChannel(channels, assertedQueues);

      expect(channels.default).not.to.be.undefined;
      expect(assertedQueues[connectionType][queueName]).not.to.be.undefined;

      rabbitMq.getChannel().emit('close');
      expect(channels.default).to.be.undefined;
      expect(assertedQueues[connectionType][queueName]).to.be.undefined;
    });

  });
});
