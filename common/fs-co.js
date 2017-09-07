var co = require('co');
var fs = require('fs');

co(function *(){

  var a = yield read('.gitignore');
  console.log(a.length);

  var b = yield read('package.json');
  console.log(b.length);
});

function read(file) {
  return function(fn){
    fs.readFile(file, 'utf8', function(err,result){
        if (err) return fn(err);
        fn(null, result);
    });
  }
}