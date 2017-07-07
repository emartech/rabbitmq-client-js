'use strict';

const chai = require('chai');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);

const expect = chai.expect;


const RetryableError = require('./retryable-error');

describe('RetryableError', function() {

  describe('#new', function() {
    it('should create and return a RetryableError', function() {
      const error = new RetryableError('Houston', 1234);
      expect(error).to.instanceOf(Error);
      expect(error).to.instanceOf(RetryableError);
      expect(error.message).to.eql('Houston');
      expect(error.code).to.eql(1234);
      expect(error.retryable).to.be.true;
    });
  });

  describe('#decorate', function() {
    it('should modify an error to retryable', function() {
      const error = new Error('Houston', 1234);
      RetryableError.decorate(error);
      expect(error.retryable).to.be.true;
    });
    it('should modify and return an error to retryable', function() {
      const error = new Error('Houston', 1234);
      const retryableError = RetryableError.decorate(error);
      expect(retryableError.retryable).to.be.true;
    });
  });

  describe('#isRetryable', function() {
    it('should return false for a general error', function() {
      const error = new Error('Houston', 1234);
      expect(RetryableError.isRetryable(error)).to.be.false;
    });
    it('should return true for a retryable error', function() {
      const error = new RetryableError('Houston', 1234);
      expect(RetryableError.isRetryable(error)).to.be.true;
    });
    it('should return true for a decorated error', function() {
      const error = new Error('Houston', 1234);
      RetryableError.decorate(error);
      expect(RetryableError.isRetryable(error)).to.be.true;
    });
  });

});
