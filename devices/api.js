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


//*** Pull Subscription ***//
const projectId = 'your-gcloud-project-id';
const subscriptionName = 'your-subscription-name';
const keyFilename = '/path/to/your/service-account-auth-key.json';

// Instantiates a client
const pubsub = PubSub({
  projectId: projectId,
  keyFilename: keyFilename
});

// Event handler for PubSub messages
const subscription = pubsub.subscription(subscriptionName);
let messageCount = 0;
const messageHandler = (message) => {
  console.log(`Received message ${message.id}:`);
  console.log(`\tData: ${message.data}`);
  console.log(`\tAttributes: ${message.attributes}`);

  console.log("message.data.usages: ", message.data && message.data.usages)
  console.log("message.data.usages: ", message.data && message.data.events)
  messageCount += 1;

  // "Ack" (acknowledge receipt of) the message
  message.ack();

  //save to db
  addDeviceData(message.data, message.publishTime, next);
};

// Listen for new messages until timeout is hit
subscription.on(`message`, messageHandler);
setTimeout(() => {
  subscription.removeListener('message', messageHandler);
  console.log(`${messageCount} message(s) received.`);
}, 30000 * 1000);

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

const router = express.Router();
// Automatically parse request body as JSON
router.use(bodyParser.json());

//Telemetry Push Subscription Web hook
router.post('/_ah/push-handlers/time-series/telemetry', (req, res, next) => {
  const reqBody = req && req.body;
  const entryData = reqBody &&
    reqBody.message &&
    reqBody.message.data;
  let decodedData;
  let dataObj;
  let entry;

  if (entryData) {
    decodedData = Buffer.from(entryData, 'base64');
    dataObj = JSON.parse(decodedData.toString());

    addDeviceData(dataObj, reqBody.publishTime, next);

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
