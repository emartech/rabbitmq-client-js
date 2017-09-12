'use strict';

const amqp = require('amqplib');
const Pool = require('./pool');
const EventEmitter = require('events');


const config = {
  default: {
    url: 'amqp://test:secret@192.168.40.10:5672/cubebloc'
  }
};
const queueName = 'test-queue';

describe('RabbitMQ', function() {
  let rabbitMq;
  let sandbox;

  let connectionMock;
  let channelMock;

  beforeEach(async function() {
    sandbox = this.sandbox;

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
    rabbitMq = Pool.create(config, 'default', false).getClient(queueName, { durable: true, autoDelete: true });
  });

  it('#connect should call amqp connect with rigth parameters', async function() {
    await rabbitMq.connect();
    expect(amqp.connect).to.have.been.calledWith(
      'amqp://test:secret@192.168.40.10:5672/cubebloc',
      { servername: '192.168.40.10' }
    );
  });

  it('#connect cache the connection', async function() {
    const connections = {};
    await rabbitMq.connect(connections);
    await rabbitMq.connect(connections);

    expect(amqp.connect).to.have.been.calledOnce;
  });

  it('#createChannel should check if connection is ready', async function() {
    await expect(rabbitMq.createChannel()).to.be.rejectedWith('No RabbitMQ connection');
    await rabbitMq.connect();
    await expect(rabbitMq.createChannel()).to.be.fulfilled;
  });

  it('#createChannel should cache the channel and assert the queue', async function() {
    const assertQueueValue = { testing: 123 };
    channelMock.assertQueue = sandbox.stub().resolves(assertQueueValue);

    await rabbitMq.connect();
    await rabbitMq.createChannel();
    await rabbitMq.createChannel();

    expect(channelMock.assertQueue).to.have.been.calledWith(queueName, { durable: true, autoDelete: true });
    expect(channelMock.assertQueue).to.have.been.calledOnce;
    expect(connectionMock.createChannel).to.have.been.calledOnce;
  });

  it('#createChannel should check if queueName was set', async function() {
    rabbitMq = Pool.create(config, 'default', false).getClient();
    await rabbitMq.connect();
    await expect(rabbitMq.createChannel()).to.be.rejectedWith('No RabbitMQ queue');
  });

  it('#insert should call sentToQueue', async function() {
    const data = { test: 'data' };
    await rabbitMq.connect();
    await rabbitMq.createChannel();
    rabbitMq.insert(data);
    expect(channelMock.sendToQueue).to.have.been.calledWith(queueName, new Buffer(JSON.stringify(data)));
  });

  it('#insertWithGroupBy should call sentToQueue', async function() {
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

  it('#purge should empty the queue', async function() {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.purge();

    expect(channelMock.purgeQueue).to.have.been.calledWith(queueName);
  });

  it('#closeConnection should close the rabbitMq connection', async function() {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.closeConnection();
    expect(connectionMock.close).to.have.been.calledOnce;
  });

  it('#destroy should delete the queue', async function() {
    await rabbitMq.connect();
    await rabbitMq.createChannel();

    await rabbitMq.destroy();
    expect(channelMock.deleteQueue).to.have.been.calledWith(queueName);
  });
});
