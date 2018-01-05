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

// Your Google Cloud Platform project ID
// const projectId = 'kevins-club-1510687140500';
const projectId = 'candi-dev';
const subscriptionName = 'Dyer-Club-PULL.APT_GOOGLE_PUBSUB.subscription.114';
const keyFilename = '/Users/kevind/candi/dyers-club/google-cloud-auth.json';

// Instantiates a client
const pubsub = PubSub({
  projectId: projectId,
  keyFilename: keyFilename
});

const subscription = pubsub.subscription(subscriptionName);

// Create an event handler to handle messages
let messageCount = 0;
const messageHandler = (message) => {
  console.log(`Received message ${message.id}:`);
  console.log(`\tData: ${message.data}`);
  console.log(`\tAttributes: ${message.attributes}`);
  messageCount += 1;

  // "Ack" (acknowledge receipt of) the message
  message.ack();
};

// Listen for new messages until timeout is hit
subscription.on(`message`, messageHandler);
setTimeout(() => {
  subscription.removeListener('message', messageHandler);
  console.log(`${messageCount} message(s) received.`);
}, 30000 * 1000);


function getModel () {
  return require(`./model-${require('../config').get('DATA_BACKEND')}`);
}


function getEntryKind (entry) {
  return `${entry.siteCd}-${entry.deviceCd}-${entry.usageType}-${entry.intervalType}`
}
function getStartOfDayEpoch (entry) {
  const date = moment.utc(entry.timestamp).format('YYYY-MM-DD')
  return moment.utc(date).valueOf()
}

//stream could be either events or usages
function updateDeviceData (streams, reqBody) {
  streams.map(stream => {
    console.log("reqBody.message: ", reqBody.message);
    return {
      ...stream, //includes intervalType, usageType, timestamp, and value
      siteCd: reqBody.message.attributes && reqBody.message.attributes.siteCd, //empty???
      gatewayCd: reqBody.message.attributes && reqBody.message.attributes.gatewayCd,
    }
  })
  // .filter(entry => getEntryKind(entry))
  .forEach(entry => {
    //find existing entry
    //NOTE: first arg is id - but note: need to have different key for each device and usage type

    console.log("entry: ", entry, ", getEntryKind(entry): ", getEntryKind(entry), ", getStartOfDayEpoch(entry): ", getStartOfDayEpoch(entry))
    let nextEntry
    getModel().read(getEntryKind(entry), getStartOfDayEpoch(entry), (err, existingEntry) => {

      if (existingEntry) {
        nextEntry = {
          ...existingEntry,
          values: {
            ...existingEntry.values || {},
            [parseInt(entry.timestamp)]: entry.value
          }
        }
      } else {
        nextEntry = {
          ...entry,
          values: {
            [parseInt(entry.timestamp)]: entry.value
          }
        }

        delete nextEntry.value
      }

      //TODO: save nextEntry
      getModel().update(getEntryKind(nextEntry), getStartOfDayEpoch(nextEntry), nextEntry, (err, updatedEntry) => {
        if (err) {
          next(err);
          return;
        }
      });
    });
  });
}

const router = express.Router();

// Automatically parse request body as JSON
router.use(bodyParser.json());

/**
 * GET /api/books
 *
 * Retrieve a page of books (up to ten at a time).
 */
// router.get('/', (req, res, next) => {
//   getModel().list(10, req.query.pageToken, (err, entities, cursor) => {
//     if (err) {
//       next(err);
//       return;
//     }
//     res.json({
//       items: entities,
//       nextPageToken: cursor
//     });
//   });
// });

// /**
//  * POST /api/books
//  *
//  * Create a new book.
//  */
// router.post('/', (req, res, next) => {
//   getModel().create(req.body, (err, entity) => {
//     if (err) {
//       next(err);
//       return;
//     }
//     res.json(entity);
//   });
// });

// /**
//  * GET /api/books/:id
//  *
//  * Retrieve a book.
//  */
// router.get('/:book', (req, res, next) => {
//   getModel().read(req.params.book, (err, entity) => {
//     if (err) {
//       next(err);
//       return;
//     }
//     res.json(entity);
//   });
// });

// /**
//  * PUT /api/books/:id
//  *
//  * Update a book.
//  */
// router.put('/:book', (req, res, next) => {
//   getModel().update(req.params.book, req.body, (err, entity) => {
//     if (err) {
//       next(err);
//       return;
//     }
//     res.json(entity);
//   });
// });

// /**
//  * DELETE /api/books/:id
//  *
//  * Delete a book.
//  */
// router.delete('/:book', (req, res, next) => {
//   getModel().delete(req.params.book, (err) => {
//     if (err) {
//       next(err);
//       return;
//     }
//     res.status(200).send('OK');
//   });
// });

//Telemetry Push Subscription Web hook
router.post('/_ah/push-handlers/time-series/telemetry', (req, res, next) => {
  const reqBody = req && req.body;
  const entryData = reqBody &&
    reqBody.message &&
    reqBody.message.data;
  let decodedData;
  let dataObj;
  let entry;

  // console.log("reqBody: ", reqBody)
  // console.log("entryData: ", entryData)

  if (entryData) {
    decodedData = Buffer.from(entryData, 'base64');
    dataObj = JSON.parse(decodedData.toString());

    // TODO: create a key from the siteCd, deviceCd, usageType and dateRange, 

    //TODO: update
    // entry = Object.assign({}, reqBody.attributes, {createdAt: Date.now()})
    // console.log("reqBody: ", reqBody);
    // console.log("Object.keys(reqBody.message): ", Object.keys(reqBody.message));
    // console.log("reqBody.message.attributes: ", reqBody.message.attributes);
    // console.log("reqBody.message.attributes.siteCd: ", reqBody.message.attributes && reqBody.message.attributes.siteCd);
    console.log("entryData: ", entryData);
    console.log("dataObj: ", dataObj);
    console.log("dataObj.usages: ", dataObj.usages);
    console.log("dataObj.events: ", dataObj.events);

    if (dataObj.usages) {
      updateDeviceData(dataObj.usages, reqBody);
    }
    if (dataObj.events) {
      updateDeviceData(dataObj.events, reqBody);
    }

    res.status(200).send('OK');
  } else {
    console.log("No dataObject was found!")
    
    res.status(204).send()
  }

  //TODO: loop through dataObj.usages, get resource, update with new data, and resave

  //NOTE: this needs to be updated
})

/**
 * Errors on "/api/books/*" routes.
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