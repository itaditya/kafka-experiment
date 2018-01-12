const kafkaService = require('./kafka_service.js');

kafkaService.sendRecord({
  userId: 'adi'
}, (...args) => {
  console.log(args);
})
