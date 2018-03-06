// Copyright 2017, Google, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

const express = require('express');
const bodyParser = require('body-parser');
const moment = require('moment');
// Imports the Google Cloud client library
const PubSub = require('@google-cloud/pubsub');
const BigQuery = require('@google-cloud/bigquery');


//TODO: replace with generic values for example code
//*** Pull Subscription ***//
// const projectId = 'your-gcloud-project-id';
// const subscriptionName = 'your-subscription-name';
// const keyFilename = '/path/to/your/service-account-auth-key.json';

const projectId = 'kevins-club-1510687140500';
const subscriptionName = 'your-subscription-name';
const keyFilename = '/Users/kevind/candi/pubSub-Node-example/kevins-club-bd3448fdbf74.json';
const datasetId = 'deviceTelemetry';
const tableId = 'timeSeries';

// Instantiates a client
const pubsub = PubSub({
  projectId: projectId
  // keyFilename: keyFilename
});

// Creates a client
const bigquery = new BigQuery({
  projectId: projectId
  // keyFilename: keyFilename
});


//TODO: uncomment
// Event handler for PubSub messages
// const subscription = pubsub.subscription(subscriptionName);
// let messageCount = 0;
// const messageHandler = (message) => {
//   console.log(`Received message ${message.id}:`);
//   console.log(`\tData: ${message.data}`);
//   console.log(`\tAttributes: ${message.attributes}`);

//   console.log("message.data.usages: ", message.data && message.data.usages)
//   console.log("message.data.usages: ", message.data && message.data.events)
//   messageCount += 1;

//   // "Ack" (acknowledge receipt of) the message
//   message.ack();

//   //save to db
//   addDeviceData(message.data, message.publishTime, next);
// };

// // Listen for new messages until timeout is hit
// subscription.on(`message`, messageHandler);
// setTimeout(() => {
//   subscription.removeListener('message', messageHandler);
//   console.log(`${messageCount} message(s) received.`);
// }, 30000 * 1000);

//*** END of PULL Subscription ***//


function getModel () {
  return require(`./model-${require('../config').get('DATA_BACKEND')}`);
}

//Save data packet to data base
function addDeviceData (data, publishTime, next) {
  getModel().create({
    ...data,
    publishTime: publishTime || moment().format()
  }, (err, entity) => {
    if (err) {
      next(err);
      return;
    }
  })
}

//Save each data point to bigquery
function saveToBigQuery (dataPacket={}, publishTime, attributes={}) {
  const {usages = [], events=[]} = dataPacket || {}

  //TODO: add events if they exist
  const rows = [...usages].map(dataPoint => {
    return {
      deviceCd: dataPoint.deviceCd,
      usageType: dataPoint.usageType,
      intervalType: dataPoint.intervalType,
      value: dataPoint.value,
      timestamp: dataPoint.timestamp,
      siteCd: attributes.siteCd,
      gatewayCd: attributes.gatewayCd,
      publishTime: publishTime || moment().format()
    }
  })
  
  if (rows && rows.length > 0) {
    console.log("inserting rows: ", rows)
    bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(rows, (err, apiResponse) => {
        if (err) {
          console.error("An API error or partial failure occurred.");

          if (err.name === 'PartialFailureError') {
            console.error("PartialFailureError: Some rows failed to insert, while others may have succeeded.");
            console.log("err.errors[0]: ", err.errors[0])
            // err.errors (object[]):
            // err.errors[].row (original row object passed to `insert`)
            // err.errors[].errors[].reason
            // err.errors[].errors[].message
          }
        }
      })
  }
}

const router = express.Router();
// Automatically parse request body as JSON
router.use(bodyParser.json({
  limit: '50mb',
  parameterLimit: 100000,
  type: 'application/json'}));
router.use(bodyParser.urlencoded({
  parameterLimit: 100000,
  limit: '50mb',
  extended: true
}));

//Telemetry Push Subscription Web hook
router.post('/_ah/push-handlers/time-series/telemetry', (req, res, next) => {
  const reqBody = req && req.body;
  const entryData = reqBody &&
    reqBody.message &&
    reqBody.message.data;
  const attributes = reqBody &&
    reqBody.message &&
    reqBody.message.attributes || {}
  let decodedData;
  let dataObj;
  let entry;

  if (entryData) {
    decodedData = Buffer.from(entryData, 'base64');
    dataObj = JSON.parse(decodedData.toString());

    console.log("dataObj dataObj: ", dataObj)
    // console.log("dataObj usages: ", dataObj && dataObj.usages)
    // console.log("reqBody.messages.attributes: ", attributes)
    console.log("--")
    console.log("--")

    if (dataObj) {
      saveToBigQuery(dataObj, reqBody.publishTime, attributes)
      addDeviceData(dataObj, reqBody.publishTime, next);
    }

    res.status(200).send('OK');
  } else {
    console.log("No dataObject was found!")
    
    res.status(204).send()
  }
})

/**
 * Errors on "/api/devices/*" routes.
 */
router.use((err, req, res, next) => {
  // Format error and forward to generic error handler for logging and
  // responding to the request
  err.response = {
    message: err.message,
    internalCode: err.code
  };
  next(err);
});

module.exports = router;
