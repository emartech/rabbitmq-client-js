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
  const sandbox = sinon.sandbox.create();

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
    const logger = getLogger('test');
    logger.info('test', { message: 'fake' });
    expect(Logger.prototype.info).to.have.been.calledOnce;
    expect(Logger.prototype.info).to.have.been.calledWith('test', { message: 'fake' });
  });
});
