#!/usr/bin/env node

const amqp = require('amqplib/callback_api');
const axios = require('axios');

const RMQ_USER = 'covid-api-user';
const RMQ_PASSWORD = '9j%L*9U4CewmGZZ6u8zf';
const RMQ_HOST = 'b-fccd3130-419b-4c98-b0d3-421707f92cbd.mq.sa-east-1.amazonaws.com';
const RMQ_PORT = '5671';

const COVID_API_URL = 'https://covid19-brazil-api.now.sh/api/report/v1';

const QUEUE_GENERAL = 'reports_queue_general';
const QUEUE_COUNTRIES = 'reports_queue_countries';
const util = require('util');

let queueChannel;

const getDataFromCovidAPIGeneral = async () => {
    const resp = await axios.get(COVID_API_URL)
        .then(response => {
            // console.log('Brazil')
            // console.log(util.inspect(response.data, {showHidden: false, depth: null}))
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
            // console.log('Countries')
            // console.log(util.inspect(response.data, {showHidden: false, depth: null}))
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

        console.log(queue)
        console.log(report.data)
        console.log(JSON.stringify(report))
        // console.log(util.inspect(response.data, {showHidden: false, depth: null}))

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

setInterval(async () => {
    const reportGeneral = await getDataFromCovidAPIGeneral();
    const reportCountries = await getDataFromCovidAPICountries();
    
    if (reportGeneral) {
        publishToQueue(reportGeneral, QUEUE_GENERAL);
    }

    if (reportCountries) {
        publishToQueue(reportCountries, QUEUE_COUNTRIES);
    }
}, 3000);