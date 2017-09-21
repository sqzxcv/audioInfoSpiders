const bluebird = require("bluebird")
const request_koa = require("./common/request_koa.js")
const mysql = require('mysql')
bluebird.promisifyAll(require("mysql/lib/Connection").prototype);
bluebird.promisifyAll(require("mysql/lib/PoolConnection").prototype)
bluebird.promisifyAll(require("mysql/lib/Pool").prototype);
const moment = require("moment")
const sqlStringM = require('sqlstring')
const config = require('./config')

var connection = null
var pool = null
const makeRadioDB = async() => {

    var pool = mysql.createPool({
        host: config['dbhost'],
        user: config['dbuser'],
        password: config['dbpwd'],
        database: "Nina",
        connectionLimit: 100,
        port: config['dbport'],
        waitForConnections: false
    });

    try {
        connection = await pool.getConnectionAsync();
    } catch (error) {
        await connection.release()
        await pool.endAsync();
        console.error(error)
        console.error("创建数据库链接失败")
        return
    }
    var catalog_names = ['科技', '数码', '汽车', "产经", "房产", "股票", "金融", "娱乐", "体育", "脱口秀", "国内","国际","军事","社会"]
    for (var index = 0; index < catalog_names.length; index++) {
        var catalog_name = catalog_names[index];
        var sql = `select news_id, catalog_name, catalog_id,image, duration, summary, text, tags, source, hot,news_time,title,audio,collect_time, catalogid from radioDB_bk where catalog_name='${catalog_name}' order by news_time desc`
        try {
            var insertResults = await connection.queryAsync(sql)
            await insertToRadioDB(insertResults)
        } catch (error) {
            console.error(error)
        }
    }
    console.log(`====================所有记录 更新完成====================`)
}

const insertToRadioDB = async(results) => {

    var duration_dat = 15 * 60
    var name = ""
    for (var index = 0; index < results.length; index++) {
        var el = results[index]
        name = el.catalog_name
        var insertSql = `insert ignore into radioDB(news_id, catalog_name, catalog_id,image, duration, summary, text, tags, source, hot,news_time,title,audio,collect_time, catalogid) values`
        insertSql += `('${el.news_id}', ${sqlStringM.escape(el.catalog_name)}, '${el.catalog_id}', '${el.image}', ${el.duration}, ${sqlStringM.escape(el.summary)}, ${sqlStringM.escape(el.text)}, ${sqlStringM.escape(el.tags)}, ${sqlStringM.escape(el.source)}, ${el.hot}, ${el.news_time}, ${sqlStringM.escape(el.title)}, ${sqlStringM.escape(el.audio)}, ${moment().unix() + duration_dat * (index + 1) + 60*60*24}, ${el.catalogid})`
        try {
            var insertResults = await connection.queryAsync(insertSql)
            console.log(`${moment()}------ ${el.catalog_name} 更新 ${insertResults.affectedRows} 条记录`)
        } catch (error) {
            console.error(error);
        }
    }
    console.log(`====================${name}    更新完成====================`)
}

makeRadioDB();