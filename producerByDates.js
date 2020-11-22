#!/usr/bin/env node

const amqp = require('amqplib/callback_api');
const axios = require('axios');

const RMQ_USER = "xrqojakv";
const RMQ_PASSWORD = "kRsl-c5TlomUCCHpM32CHKyMtXHq_i_X";
const RMQ_HOST = "toad.rmq.cloudamqp.com/xrqojakv";

const COVID_API_URL = 'https://covid19-brazil-api.now.sh/api/report/v1';

const QUEUE_BY_DATE = 'reports_queue_general_by_date';

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

const loadCovidData = async () => {
    let startDate = new Date("2020-02-01");
    while (startDate.getTime() < getYesterdayTimestamp()) {
        await sleep(2 * 60 * 1000) // sleep for 2 min
        const dateString = startDate.toISOString().split("T")[0].replace("-", "").replace("-", "");
        const reportGeneral = await getDataFromCovidAPIGeneralSpecificDate(dateString);
        
        if (reportGeneral) {
            await publishToQueue(reportGeneral, QUEUE_BY_DATE);
        }

        console.log(`${startDate.toISOString()} sent to queue`);
        startDate.setDate(startDate.getDate()+1);
    }
}

loadCovidData();