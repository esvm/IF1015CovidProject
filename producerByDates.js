#!/usr/bin/env node

const amqp = require('amqplib/callback_api');
const axios = require('axios');

const RMQ_USER = 'covid-api-user';
const RMQ_PASSWORD = '9j%L*9U4CewmGZZ6u8zf';
const RMQ_HOST = 'b-fccd3130-419b-4c98-b0d3-421707f92cbd.mq.sa-east-1.amazonaws.com';
const RMQ_PORT = '5671';

const COVID_API_URL = 'https://covid19-brazil-api.now.sh/api/report/v1';

const QUEUE_BY_DATE = 'reports_QUEUE_BY_DATE_by_date';

let queueChannel;

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

amqp.connect(`amqps://${RMQ_USER}:${RMQ_PASSWORD}@${RMQ_HOST}:${RMQ_PORT}`, (err, connection) => {
    if (err) throw err;

    connection.createChannel((err, channel) => {
        if (err) throw err;
        queueChannel = channel;
    });
});

const loadCovidData = async () => {
    let startDate = new Date("2020-02-01");
    while (startDate < Date.now()) {
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