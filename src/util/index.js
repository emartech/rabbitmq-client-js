'use strict';

const Logger = require('@emartech/json-logger');

const getLogger = name => {
  let logger = name;
  if (typeof name === 'string') {
    Logger.configure({
      // keep event field for compatibility with logmatic alerts
      transformers: [input => Object.assign({ event: input.action }, input)]
    });
    logger = Logger(name);
  }
  return logger;
};

module.exports = { getLogger };
