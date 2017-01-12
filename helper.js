var wikipedia = require('wtf_wikipedia')
var elasticsearch = require('elasticsearch');
var kue = require('kue');

var jobs = kue.createQueue({ prefix: 'import' });
var client = new elasticsearch.Client({
  host: 'localhost:9200'
  // log: 'trace'
});

//this method may run in it's own process
exports.processScript = function (options, cb) {
  var data = wikipedia.parse(options.script)
  data.title = options.title
  options.collection.insert(data, function (e) {
    if (e) {
      console.log(e)
      return cb(e)
    }
    return cb()
  })
}


exports.esProcessScript = function (options, cb) {
  var data = wikipedia.parse(options.data.script);
  data.title = options.title;

  // console.log(options.index);
  // if (data.type === 'page') {
  client.create({
    index: 'en_wikipedia',
    type: data.type,
    id: options.id,
    timeout: '60000ms',
    body: data
  }, function (error, response) {
    if (error) {
      console.log(error);
      return cb(error);
    }
    return cb();
  });
  // }
}