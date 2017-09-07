var request = require("request");
var co = require("co");

function requestPromise(options) {
  return new Promise(function (resolve, reject) {
    request(options, function(err, response, body) {
      if (err) reject(err);
      resolve({'body':body, 'response':response});
    });
  });
}
module.exports = requestPromise;