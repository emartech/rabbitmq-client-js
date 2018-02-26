'use strict';

const Logger = require('@emartech/json-logger');

const getLogger = name => {
  let logger = name;
  if (typeof name === 'string') {
    Logger.configure({
      formatter: Logger.formatter.logentries
    });
    logger = Logger(name);
  }
  return logger;
};

module.exports = { getLogger };
