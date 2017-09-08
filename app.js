const event = new(require('events').EventEmitter)();
const nodemailer = require("nodemailer");
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

const main = async() => {
    // 
    //create and start schedule

    pool = mysql.createPool({
        host: config['dbhost'],
        user: config['dbuser'],
        password: config['dbpwd'],
        database: "Nina",
        connectionLimit: 100,
        port: "3306",
        waitForConnections: false
    });

    try {
        connection = await pool.getConnectionAsync();
    } catch (error) {
        console.error(error)
        console.error("创建数据库链接失败")
        return
    }

    //job pool
    startIntervalScheduleJob(fetchJobPool, 20 * 60)

}

var catalogPool = {}
const fetchJobPool = async() => {
    console.log("++++++++++++++++++++++++开始更新全部数据++++++++++++++++++++++++")
    var opt = {}
    opt.url = "https://api.leting.io/v1.1/catalog/list?parent_id=c12ddc8c-70a5-467f-8a6f-01ffc8a06635"
    try {
        var res = await request_koa(opt)
        if (JSON.parse(res.body).message == "Success") {
            var tpool = JSON.parse(res.body).results
            for (var index = 0; index < tpool.length; index++) {
                var element = tpool[index];
                catalogPool[element.catalog_id] = element
                var catalog_id = element.catalog_id
                fetchDataJob(fetchData, 3, {
                    "catalog_id": catalog_id,
                    "catelog_name": element.catalog_name,
                    "offset": 0,
                    "limit": 50
                });
                await sleep(3)
            }
        }
    } catch (error) {
        console.error(error)
    }
}

const startIntervalScheduleJob = async(fun, interval) => {

    await fun()
    while (1) {
        await sleep(interval)
        await fun()
    }
}

const fetchDataJob = async(fun, interval, param) => {

    var retrycount = 0
    var result = null
    while (1) {
        if (retrycount < 3) {
            var nextparam = JSON.parse(JSON.stringify(param))
            result = await fun(param)
            if (result.error == null && result.continue_fetch == true) {
                //请求成功,请求下一页,并跳出当前循环
                await sleep(interval)
                nextparam.offset = nextparam.offset + nextparam.limit
                fetchDataJob(fun, interval, nextparam)
                break
            } else if (result.continue_fetch == false) {
                console.log(`---------------------------------------------<<${param.catelog_name}>> 已经更新完成--------------------------`)
                break
            }
            //请求失败,进行重试
            retrycount++
        } else {
            //三次尝试失败, 发送邮件通知
            sendEmail(result.error)
            console.error(`${param.catelog_name} 更新遇到错误, 停止更新;error:\n${result.error}`)
            break
        }
    }
}

const fetchData = async(param) => {

    // console.log("开始查询 limit:" + param.limit + ";offset:" + param.offset)
    var url = `https://api.leting.io/v1.1/news/list?uid=3b1858a0-36b6-29be-9ff8-20558aa0f1fc&limit=${param.limit}&offset=${param.offset}&catalog_id=${param.catalog_id}`

    try {
        var res = await request_koa({
            url: url
        })
        if (JSON.parse(res.body).message == "Success") {
            var results = JSON.parse(res.body).results
            var saveInfo = await saveData2db(results)
            return saveInfo
        } else {
            return {
                "error": res.body
            }
        }
    } catch (error) {
        return {
            "error": error
        }
    }
}

/**
 * catalogid 目录 id
 * 1=科技数码:科技 + 数码 + 汽车
 * 2=金融股票:产经 + 房产 + 股票 + 金融
 * 3=娱乐 + 体育 + 脱口秀
 * 4=社会+军事:国内 + 国际 + 军事 + 社会
 * 5=其他:
 * @param  {} results 需要插入数据库的记录
 * 返回值:1 = 插入成功,继续; 0 = 插入成功,但是超过10条重复记录,需要停止抓取;
 *  -1 =插入失败, 比如数据库插入失败
 */
const saveData2db = async(results) => {

    if (results == null || results.length == 0) {
        return {
            // "error": "请求完成,退出"
            "continue_fetch": false
        }
    }

    if (connection == null) {
        console.error("数据库链接为 null")
        return {
            "continue_fetch": false
        }
    }

    var catalogid = 0
    switch (catalog_name) {
        case '科技':
        case '数码':
        case '汽车':
            catalogid = 1
            break;
        case "产经":
        case "房产":
        case "股票":
        case "金融":
            catalogid = 2
            break;
        case "娱乐":
        case "体育":
        case "脱口秀":
            catalogid = 3
            break
        case "国内":
        case "国际":
        case "军事":
        case "社会":
            catalogid = 4
            break
        default:
            catalogid = 0
            break;
    }
    var sql = `insert ignore into radioDB(news_id, catalog_name, catalog_id,image, duration, summary, text, tags, source, hot,news_time,title,audio,collect_time, catalogid) values`
    var el = results[0]
    var catalog_name = catalogPool[el.catalog_id]["catalog_name"]
    for (var index = 0; index < results.length; index++) {
        var el = results[index]
        var newtime = el.updated_at
        if (newtime.length == 0) {
            newtime = el.created_at
        }
        newtime = newtime.replace(/CST/g, "").replace(/UTC/g, "");
        if (index != 0) {
            sql += `, `
        }
        sql += `('${el.news_id}', ${sqlStringM.escape(catalog_name)}, '${el.catalog_id}', '${el.image}', ${el.duration}, ${sqlStringM.escape(el.summary)}, ${sqlStringM.escape(el.text)}, ${sqlStringM.escape(el.tags)}, ${sqlStringM.escape(el.source)}, ${el.hot}, ${moment(newtime).unix()}, ${sqlStringM.escape(el.title)}, ${sqlStringM.escape(el.audio)}, ${moment().unix()}, ${catalogid})`
    }
    var returnVal = {}
    try {
        var insertResults = await connection.queryAsync(sql)
        console.log(`${catalog_name} 更新 ${insertResults.affectedRows} 条记录`)
        if (insertResults.affectedRows < results.length - 10) {
            console.log("插入成功, 但超过10条重复记录,需要停止查询")
            returnVal = {
                "continue_fetch": false
            }
        } else {
            console.log("插入成功")
            returnVal = {
                "continue_fetch": true
            }
        }
    } catch (error) {
        console.error(error);
        returnVal = {
            "error": error
        }
    }
    return returnVal
}

var sleep = function (time) {
    return new Promise(function (resolve, reject) {
        setTimeout(function () {
            resolve();
        }, time * 1000);
    })
};

main()

function sendEmail(content) {

    var text;
    if (content.length != 0) {
        text = `LT 音频内容抓取失败:${content}`; //"本次总共采集到" + content.length + "篇文章,具体标题如下:\n" + content.join('\n');
    } else {
        text = "LT 音频内容抓取失败";
    }
    var nodemailer = require('nodemailer');
    var transporter = nodemailer.createTransport({
        service: 'QQ',
        auth: {
            user: '124561376@qq.com',
            pass: 'kagfjaiaacmebgjf'
        }
    });
    var mailOptions = {
        from: '124561376@qq.com ', // sender address
        to: '124561376@qq.com', // list of receivers
        subject: 'LT 音频内容抓取失败', // Subject line
        text: text, // plaintext body
        // html: `微信扫描登录<br/><img src="${content}">`
    };

    transporter.sendMail(mailOptions, function (error, info) {
        if (error) {
            console.log(error);
        } else {
            console.log('Message sent: ' + info.response);
        }
    });
}