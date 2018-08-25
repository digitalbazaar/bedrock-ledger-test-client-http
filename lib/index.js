/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {config} = require('bedrock');
const logger = require('./logger');
const os = require('os');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});

// module API
const api = {};
module.exports = api;

// used by secondaries to get the genesis block
api.getGenesis = callback => request({
  method: 'GET',
  url: `${config['ledger-test'].primaryBaseUrl}/genesis`,
}, (err, res) => {
  if(err || res.statusCode !== 200) {
    logger.debug('Error retrieving genesis block.');
    if(res) {
      logger.debug('Error', {
        statusCode: res.statusCode,
        body: res.body
      });
    }
    if(err) {
      logger.debug(err.toString());
    }
    return callback(new Error('Could not retrieve genesis block.'));
  }
  callback(null, res.body);
});

// used by primaries and secondaries to send status
api.sendStatus = ({label, ledgerNodeId, publicHostname}, callback) => {
  logger.debug('Sending status.', {url: config['ledger-test'].primaryBaseUrl});
  const baseUri = config.server.baseUri;
  return async.auto({
    duration: callback => cache.client.mget(
      `t|aggregate|${ledgerNodeId}`,
      `t|findConsensus|${ledgerNodeId}`,
      `t|recentHistoryMergeOnly|${ledgerNodeId}`,
      (err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, {
          aggregate: parseInt(result[0], 10) || 0,
          findConsensus: parseInt(result[1], 10) || 0,
          recentHistoryMergeOnly: parseInt(result[2], 10) || 0,
        });
      }
    ),
    opsPerSecond: callback => {
      // local events per second
      const thisSecond = Math.round(Date.now() / 1000);
      const lni = _lni(ledgerNodeId);
      const maxSeconds = 600;
      const ocl = [];
      const ocp = [];
      for(let i = 1; i <= maxSeconds; ++i) {
        ocl.push(`ocl|${lni}|${thisSecond - i}`);
        ocp.push(`ocp|${lni}|${thisSecond - i}`);
      }
      cache.client.multi()
        .mget(ocl)
        .mget(ocp)
        .exec((err, result) => {
          if(err) {
            return callback(err);
          }
          const validLocal = result[0].map(i => parseInt(i, 10) || 0);
          const sumLocal = validLocal.reduce((a, b) => a + b, 0);
          const validPeer = result[1].map(i => parseInt(i, 10) || 0);
          const sumPeer = validPeer.reduce((a, b) => a + b, 0);
          // average by the number of valid samples
          callback(null, {
            local: Math.round(sumLocal / validLocal.length),
            peer: Math.round(sumPeer / validPeer.length)
          });
        });
    },
    eventsPerSecondLocal: callback => {
      // local events per second
      const thisSecond = Math.round(Date.now() / 1000);
      const lni = _lni(ledgerNodeId);
      const maxSeconds = 600;
      const op = [];
      for(let i = 1; i <= maxSeconds; ++i) {
        op.push(`ecl|${lni}|${thisSecond - i}`);
      }
      cache.client.mget(op, (err, result) => {
        if(err) {
          return callback(err);
        }
        const valid = result.map(i => parseInt(i, 10) || 0);
        const sum = valid.reduce((a, b) => a + b, 0);
        // average by the number of valid samples
        callback(null, Math.round(sum / valid.length));
      });
    },
    eventsPerSecondPeer: callback => {
      // local events per second
      const thisSecond = Math.round(Date.now() / 1000);
      const lni = _lni(ledgerNodeId);
      const maxSeconds = 600;
      const op = [];
      for(let i = 1; i <= maxSeconds; ++i) {
        op.push(`ecp|${lni}|${thisSecond - i}`);
      }
      cache.client.mget(op, (err, result) => {
        if(err) {
          return callback(err);
        }
        const valid = result.map(i => parseInt(i, 10) || 0);
        const sum = valid.reduce((a, b) => a + b, 0);
        // average by the number of valid samples
        callback(null, Math.round(sum / valid.length));
      });
    },
    ledgerNode: callback => brLedgerNode.get(null, ledgerNodeId, callback),
    creator: ['ledgerNode', (results, callback) => {
      const {ledgerNode} = results;
      ledgerNode.consensus._voters.get({ledgerNodeId: ledgerNode.id}, callback);
    }],
    avgConsensusTime: ['creator', (results, callback) => {
      const creatorId = results.creator.id;
      results.ledgerNode.storage.events.collection.aggregate([
        {$match: {
          'meta.consensus': {$exists: true}, 'meta.continuity2017.type': 'm',
          'meta.continuity2017.creator': creatorId
        }},
        {$sort: {'meta.continuity2017.generation': -1}},
        {$limit: 100},
        {$project: {
          consensusTime: {$subtract: ['$meta.consensusDate', '$meta.created']}
        }},
        {$group: {
          _id: null,
          avgConsensusTime: {$avg: '$consensusTime'}
        }}
      ]).toArray((err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          return callback(null, 0);
        }
        callback(null, result[0].avgConsensusTime);
      });
    }],
    latestSummary: ['ledgerNode', (results, callback) =>
      results.ledgerNode.storage.blocks.getLatestSummary(callback)],
    eventsOutstanding: ['ledgerNode', (results, callback) =>
      results.ledgerNode.storage.events.getCount({consensus: false}, callback)],
    eventsTotal: ['ledgerNode', (results, callback) =>
      results.ledgerNode.storage.events.getCount({}, callback)],
    mergeEventsTotal: ['ledgerNode', (results, callback) =>
      results.ledgerNode.storage.events.collection.count({
        'meta.continuity2017.type': 'm'
      }, callback)],
    mergeEventsOutstanding: ['ledgerNode', (results, callback) =>
      results.ledgerNode.storage.events.collection.count({
        'meta.continuity2017.type': 'm',
        'meta.consensus': {$exists: false}
      }, callback)],
    sendStatus: [
      'avgConsensusTime', 'duration', 'eventsTotal',
      'eventsOutstanding', 'eventsPerSecondLocal', 'eventsPerSecondPeer',
      'latestSummary', 'mergeEventsOutstanding', 'mergeEventsTotal',
      'opsPerSecond',
      ({avgConsensusTime, duration, eventsOutstanding,
        eventsPerSecondLocal, eventsPerSecondPeer, eventsTotal, latestSummary,
        mergeEventsOutstanding, mergeEventsTotal, opsPerSecond
      }, callback) => {
        request({
          body: {
            baseUri,
            // use object key safe label
            label,
            ledgerNodeId,
            // logGroupName: config.loggers.cloudwatch.logGroupName,
            logUrl: `https://${publicHostname}:${config.server.port}/log/app`,
            mongoUrl: `https://${publicHostname}:${config.server.port}/mongo`,
            privateHostname: config.server.domain,
            publicHostname,
            status: {
              latestSummary,
              duration,
              events: {
                avgConsensusTime,
                eventsPerSecondLocal,
                eventsPerSecondPeer,
                mergeEventsOutstanding,
                mergeEventsTotal,
                outstanding: eventsOutstanding,
                total: eventsTotal,
              },
              loadAverage: os.loadavg(),
              opsPerSecond,
            }
          },
          method: 'POST',
          url: `${config['ledger-test'].primaryBaseUrl}/nodes`,
          json: true,
          strictSSL: false
        }, callback);
      }],
  }, err => callback(err));
};

const urnUuidReg = /([^\:]*)\:*$/;
const allHyphenReg = /-/g;
function _lni(ledgerNodeId) {
  // return the uuid portion with hypens removed
  return ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
}
