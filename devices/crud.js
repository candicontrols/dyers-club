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
const moment = require('moment-timezone');
const _ = require('underscore');

function getModel () {
  return require(`./model-${require('../config').get('DATA_BACKEND')}`);
}

const router = express.Router();
// Automatically parse request body as form data
router.use(bodyParser.urlencoded({ extended: false, limit: '50mb' }));

// Set Content-Type for all responses for these routes
router.use((req, res, next) => {
  res.set('Content-Type', 'text/html');
  next();
});

/**
 * GET /devices
 *
 * Display a page of devices (up to 20 at a time).
 */
router.get('/', (req, res, next) => {
  getModel().list(20, req.query.pageToken, (err, entities, cursor) => {
    if (err) {
      next(err);
      return;
    }

    const nextEntities = entities.map(entity => {
      const createdAt = moment.tz(entity.publishTime, "America/Los_Angeles")

      return {
        id: entity.id,
        createdAt: createdAt.format('MMM D, YYYY h:mm:ss a'),
        publishTime: entity.publishTime,
        usageType: entity.usageType,
        siteCd: entity.siteCd,
        values: JSON.stringify(entity.usages || entity.events)
      }
    })


    res.render('devices/list.pug', {
      devices: nextEntities,
      nextPageToken: cursor
    });
  });
});

/**
 * GET /devices/:id
 *
 * Display a data packet.
 */
router.get('/:device', (req, res, next) => {
  getModel().read(req.params.device, (err, entity) => {
    if (err) {
      next(err);
      return;
    }
    res.render('devices/view.pug', {
      device: entity
    });
  });
});

/**
 * Errors on "/devices/*" routes.
 */
router.use((err, req, res, next) => {
  // Format error and forward to generic error handler for logging and
  // responding to the request
  err.response = err.message;
  next(err);
});

module.exports = router;
