'use strict';

const { getLogger } = require('./');
const chai = require('chai');
const sinon = require('sinon');
const sinonChai = require('sinon-chai');
const LoggerFactory = require('@emartech/json-logger');
const Logger = require('@emartech/json-logger').Logger;

chai.use(sinonChai);

const expect = chai.expect;

describe('#getLogger', function() {
  const sandbox = sinon.createSandbox();

  beforeEach(() => {
    sandbox.spy(Logger.prototype, 'info');
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('should use the object passed in if not string', function() {
    const logger = getLogger(LoggerFactory('test'));
    logger.info('test', { message: 'fake' });
    expect(Logger.prototype.info).to.have.been.calledOnce;
    expect(Logger.prototype.info).to.have.been.calledWith('test', { message: 'fake' });
  });

  it('should create logger instance if string passed', function() {
    process.env.DEBUG = 'test';
    const logger = getLogger('test');

    const fakeLog = sandbox.stub();
    sandbox.clock = sinon.useFakeTimers();
    LoggerFactory.configure({ output: fakeLog });

    logger.info('test', { message: 'fake' });

    expect(Logger.prototype.info).to.have.been.calledOnce;
    expect(Logger.prototype.info).to.have.been.calledWith('test', { message: 'fake' });
    expect(fakeLog).to.be.calledWith(
      '{"event":"test","name":"test","action":"test","level":30,"time":"1970-01-01T00:00:00.000Z","message":"fake"}'
    );

    delete process.env.DEBUG;
  });
});
