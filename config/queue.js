var kue = require('kue')
var queue = kue.createQueue({
  prefix: 'import',
  jobEvents: false,
  redis: {
    port: 6379,
    host: 'localhost'
    //auth: ''
  }
});

module.exports = queue;
