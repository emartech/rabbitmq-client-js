'use strict';

const amqp = require('amqplib');
const RabbitMq = require('./rabbit-mq');
const chai = require('chai');
const sinon = require('sinon');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const expect = chai.expect;

const config = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  }
};
const queueName = 'test-queue';

describe('RabbitMQ', function() {
  let subject;
  let sandbox = sinon.sandbox.create();

  let connectionMock;
  let channelMock;

  beforeEach(async function() {
    channelMock = {
      sendToQueue: sandbox.stub().returns(true),
      deleteQueue: sandbox.stub().resolves(true),
      purgeQueue: sandbox.stub().resolves(true)
    };

    connectionMock = {
      createChannel: sandbox.stub().resolves(channelMock),
      close: sandbox.stub().returns(true)
    };

    sandbox.stub(amqp, 'connect').resolves(connectionMock);
    subject = new RabbitMq(config, queueName);
  });

  afterEach(async function() {
    sandbox.restore();
  });

  it('#connect should call amqp connect with rigth parameters', async function() {
    await subject.connect();
    expect(amqp.connect).to.have.been.calledWith(
      'amqp://test:secret@192.168.40.10:5672/cubebloc',
      { servername: '192.168.40.10' }
    );
  });

  it('#connect cache the connection', async function() {
    const connections = {};
    await subject.connect(connections);
    const connection = await connections.default;

    expect(connection).to.be.equal(connectionMock);
  });

  it('#connect should reuse existing connection if it was already created', async function() {
    const localConnectionMock = {
      close: sandbox.stub().resolves(true)
    };
    const connections = { default: Promise.resolve(localConnectionMock) };
    await subject.connect(connections);

    await subject.closeConnection();
    expect(localConnectionMock.close).to.have.been.calledOnce;
  });

  it('#createChannel should check if connection is ready', async function() {
    await expect(subject.createChannel()).to.be.rejectedWith('No RabbitMQ connection');
    await subject.connect();
    await expect(subject.createChannel()).to.be.fulfilled;
  });

  it('#createChannel should cache the channel', async function() {
    const channels = {};
    await subject.connect();
    await subject.createChannel(channels);

    const channel = await channels.default;

    expect(channel).to.be.equal(channelMock);
  });

  it('#createChannel should reuse existing channel if it was already created', async function() {
    const channels = { default: Promise.resolve('channel') };

    await subject.connect();
    await subject.createChannel(channels);

    const channel = await channels.default;

    expect(subject.getChannel()).to.be.equal(channel);
  });

  it('#createChannel should check if queueName was set', async function() {
    subject = new RabbitMq(config);
    await subject.connect();
    await expect(subject.createChannel()).to.be.rejectedWith('No RabbitMQ queue');
  });

  it('#insert should call sentToQueue', async function() {
    const data = { test: 'data' };
    await subject.connect();
    await subject.createChannel();
    subject.insert(data);
    expect(channelMock.sendToQueue).to.have.been.calledWith(queueName, new Buffer(JSON.stringify(data)));
  });

  it('#insertWithGroupBy should call sentToQueue', async function() {
    const groupBy = 'me.login';
    const data = { test: 'data' };
    await subject.connect();
    await subject.createChannel();

    subject.insertWithGroupBy(groupBy, data);
    expect(channelMock.sendToQueue).to.have.been.calledWith(
      queueName,
      new Buffer(JSON.stringify(data)),
      { headers: { groupBy } }
    );
  });

  it('#purge should empty the queue', async function() {
    await subject.connect();
    await subject.createChannel();

    await subject.purge();

    expect(channelMock.purgeQueue).to.have.been.calledWith(queueName);
  });

  it('#closeConnection should close the rabbitMq connection', async function() {
    await subject.connect();
    await subject.createChannel();

    await subject.closeConnection();
    expect(connectionMock.close).to.have.been.calledOnce;
  });

  it('#destroy should delete the queue', async function() {
    await subject.connect();
    await subject.createChannel();

    await subject.destroy();
    expect(channelMock.deleteQueue).to.have.been.calledWith(queueName);
  });
});
