var config = require("./default");

//默认为开发环境
//本地调试环境
if (process.env.NODE_ENV === 'production') {

    var production = require('./production');
    config = mergeMap(config,production);
} else {

    var dev = require('./dev');
    config = mergeMap(config,dev);

}
module.exports = config;

function mergeMap(map1) {
    for (var i = 1; i < arguments.length; i++) {
        var map2 = arguments[i];
        for (var k in map2) {
            map1[k] = map2[k]
        }
    }
    //alert(JSON.stringify(map1));
    return map1;
}