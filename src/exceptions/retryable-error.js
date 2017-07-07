'use strict';

class RetryableError extends Error {

  constructor(message, code) {
    super();
    this.message = message;
    this.code = code;
    RetryableError.decorate(this);
  }

  static decorate(error) {
    error.retryable = true;
    return error;
  }

  static isRetryable(error) {
    return !!error.retryable;
  }

}

module.exports = RetryableError;
