'use strict';

const Pool = require('./pool');

describe('RabbitMQ Pool', () => {
  const amqpConfig = {
    default: {
      url: 'amqp://a:b@192.168.40.10:5672/c'
    }
  };

  describe('#getClient', () => {
    it('should return the same instance for the same connection type if using global pool', function() {
      const rabbitMqInstance1 = Pool.create(amqpConfig).getClient('test');
      const rabbitMqInstance2 = Pool.create(amqpConfig).getClient('test');

      expect(rabbitMqInstance1).to.eq(rabbitMqInstance2);
    });
  });
});
