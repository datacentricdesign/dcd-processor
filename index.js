#!/usr/bin/env node

// Load environment variables
require("dotenv").config();

// Setting the logs
const log4js = require('log4js');
const logger = log4js.getLogger('[index]');
logger.level = process.env.LOG_LEVEL || 'INFO';

const DCDModel = require("dcd-model");
const Property = require("dcd-model/entities/Property");

const propertyMap = {};
const thingMap = {};

const model = new DCDModel();
model.init()
  .then( () => {

    const topics = [
      {
        topic: 'things',
        partition: 0
      },
      {
        topic: 'properties',
        partition: 0
      },
      {
        topic: 'values',
        partition: 0
      }
    ];

    const options = {
      groupId: 'dcd-processor',
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024
    };

    model.kafka.setConsumer(topics, options, onMessage)
  });

/**
 * Handle Kafka messages
 * @param {Message} message
 */
function onMessage(message) {
  logger.debug(message);
  let json;

  try {
    json = JSON.parse(message.value.toString());
  } catch (e) {
    return logger.error('Could not parse message from topic '
      + message.topic + ' : ' + message.value);
  }

  switch (message.topic) {
    case 'things':
      processThing(json, message.key);
      break;
    case 'properties':
      processProperty(json, message.key);
      break;
    case 'values':
      processValue(message);
      break;
    default:

  }

}

function processThing(thing) {
  logger.debug('process thing');
  createThingProperties(thing.id);
}

function processProperty(property) {
  logger.debug('process property');
  if (property.entityId !== undefined
    && !propertyMap.hasOwnProperty(property.id)) {
    if (!thingMap.hasOwnProperty(property.entityId)) {
      model.things.read(property.entityId)
        .then((thing) => {
          logger.debug('read thing ' + thing.id);
          thingMap[property.entityId] = {
            id: thing.id,
            currentPeriodDataCount: 0,
            activity: 0,
            dataCountId: thing.findPropertyByName('Data Count').id,
            dataActivityId: thing.findPropertyByName('Data Activity').id
          };
        })
        .catch((error) => {
          logger.debug('error read thing: ' + error.message);
          createThingProperties(property.entityId);
        });
    }
    propertyMap[property.id] = property.entityId;
  }
}

function processValue(values, propertyId) {
  logger.debug('process values');
  if (propertyMap.hasOwnProperty(propertyId)) {
    thingMap[propertyMap[propertyId]].currentPeriodDataCount += values.length;
  }
}

function checkActivityAndCount() {
  logger.debug('check activity and count');
  for (let key in thingMap) {
    if (!thingMap.hasOwnProperty(key)) continue;
    let thing = thingMap[key];
    logger.debug(thing.id + ' ' + thing.currentPeriodDataCount + ' ' + thing.activity);
    if (thing.currentPeriodDataCount > 0) {
      // assume that we will receive a values for the count, so we start at -1
      thing.currentPeriodDataCount = -1;
      // update the count value
      model.properties.updateValues(new Property({
        id: thing.dataCountId,
        values: [[Date.now(), thing.currentPeriodDataCount]]
      }));
      // No new data point, check if we should switch the activity state to busy (1)
      if (thing.activity === 0) {
        // it was quiet, it became busy!
        // change the activity state
        thing.activity = 1;
        // decrease the data counter to escape the activity data point
        thing.currentPeriodDataCount--;
        // update the activity value
        model.properties.updateValues(new Property({
          id: thing.dataCountId,
          values: [[Date.now(), thing.activity]]
        }));
      }
    }
    // No new data point, check if we should switch the activity state to quiet (0)
    else if (thing.activity === 1) {
      // it was busy, it became quiet!
      // change the activity state
      thing.activity = 1;
      // decrease the data counter to escape the activity data point
      thing.currentPeriodDataCount--;
      // update the activity value
      model.properties.updateValues(new Property({
        id: thing.dataActivityId,
        values: [[Date.now(), thing.activity]]
      }));
    }
  }
}

function createThingProperties(entityId) {
  logger.debug('create thing prop ' + entityId);
  // let dataCountPropId, dataActivityPropId;
  model.properties
    // create a property for the data count
    .create(new Property({
      name: 'Data Count',
      type: 'COUNT',
      entityId: entityId
    }))
    // create a property for the data activity
    .then(property => {
      dataCountPropId = property.id;
      return model.properties
        .create(new Property({
          name: 'Data Activity',
          class: ['Quiet', 'Busy'],
          type: 'CLASS',
          entityId: entityId
        }))
    })
    // add the thing to the thing map
    .then((property) => {
      dataActivityPropId = property.id;
      thingMap[entityId] = {
        id: entityId,
        currentPeriodDataCount: 0,
        activity: 0,
        dataCountId: dataCountPropId,
        dataActivityId: dataActivityPropId
      };
      return Promise.resolve();
    })
    .catch(error => logger.error(error));
}

setInterval(checkActivityAndCount, process.env.CHECK_ACTIVITY_COUNT || 60000);