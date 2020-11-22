#!/usr/bin/env node

const amqp = require('amqplib/callback_api');
const axios = require('axios');

const RMQ_USER = "xrqojakv";
const RMQ_PASSWORD = "kRsl-c5TlomUCCHpM32CHKyMtXHq_i_X";
const RMQ_HOST = "toad.rmq.cloudamqp.com/xrqojakv";

const COVID_API_URL = 'https://covid19-brazil-api.now.sh/api/report/v1';

const QUEUE_GENERAL = 'reports_queue_general';
const QUEUE_COUNTRIES = 'reports_queue_countries';
const QUEUE_DEMO = "reports_queue_demo";
let shouldContinue = false

const util = require('util');

let queueChannel;

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function getYesterdayTimestamp() {
    const now = Date.now();
    const d = new Date(new Date(now).toISOString().split("T")[0]);
    const yesterday = new Date(d.setDate(d.getDate()-1)).getTime();
    return yesterday;
}

const getDataFromCovidAPIGeneral = async () => {
    const resp = await axios.get(COVID_API_URL)
        .then(response => {
            return response.data;
        })
        .catch(error => {
            console.log(error);
        });
    return resp;
}

const getDataFromCovidAPICountries = async () => {
    const resp = await axios.get(`${COVID_API_URL}/countries`)
        .then(response => {
            return response.data;
        })
        .catch(error => {
            console.log(error);
        });
    return resp;
}

const getDataFromCovidAPIGeneralSpecificDate = async (date) => {
    const resp = await axios.get(`${COVID_API_URL}/brazil/${date}`)
        .then(response => {
            return response.data;
        })
        .catch(error => {
            console.log(error);
        });
    return resp;
}

const produceDemoCovidData = async (stringDate) => { // "2020-02-01"
    let startDate = new Date(stringDate);

    while (startDate.getTime() <= getYesterdayTimestamp() && shouldContinue) {
        await sleep(3 * 1000) // sleep for 3 sec
        
        const dateString = startDate.toISOString().split("T")[0].replace("-", "").replace("-", "");
        const reportData = await getDataFromCovidAPIGeneralSpecificDate(dateString);
        
        if (reportData) {
            await publishToQueue(reportData, QUEUE_DEMO);
        }

        console.log(`${startDate.toISOString()} sent to queue`);
        startDate.setDate(startDate.getDate()+1);
    }

    shouldContinue = false;
}

const publishToQueue = (report, queue) => {
    if (!queueChannel) return;

    try {
        queueChannel.assertQueue(queue, { durable: true });
        queueChannel.sendToQueue(queue, Buffer.from(JSON.stringify(report)), { persistent: true });

        console.log('message sent to queue');
    } catch (e) {
        console.log('an error ocurred ', e);
    }
}

amqp.connect(`amqps://${RMQ_USER}:${RMQ_PASSWORD}@${RMQ_HOST}`, (err, connection) => {
    if (err) throw err;

    connection.createChannel((err, channel) => {
        if (err) throw err;
        queueChannel = channel;
    });
});

setInterval(async () => {
    const reportGeneral = await getDataFromCovidAPIGeneral();
    const reportCountries = await getDataFromCovidAPICountries();
    
    if (reportGeneral) {
        publishToQueue(reportGeneral, QUEUE_GENERAL);
    }

    if (reportCountries) {
        publishToQueue(reportCountries, QUEUE_COUNTRIES);
    }
}, 5 * 60 * 1000); // sleep for 5min


var express = require('express');
var app = express();

app.use(express.static('public'));

app.post('/demo-start', function (req, res) {
    shouldContinue = !shouldContinue;
    res.send('Demo started...');
    produceDemoCovidData(req.query.date);
});

app.post('/demo-stop', function (res, res) {
    shouldContinue = false
    res.send(`Demo ready to begin again`)
})

app.get('/', function(req, res) {
    res.send('Hello Kiev')
})

// Handle 404 - Keep this as a last route
app.use(function(req, res, next) {
    res.status(404);
    res.send('404: File Not Found');
});

app.listen(process.env.PORT || 8080, function () {
    console.log(`Example app listening on port ${process.env.PORT || 8080}!`);
});