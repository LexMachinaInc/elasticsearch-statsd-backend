const fetch = require('node-fetch')
const base64 = require('base-64')
const templates = require('./templates')
const os = require('os')

const elasticStats = {
  statusCalls: 0,
  metricCount: 0,
  inserts: 0,
  errors: 0,
  lastFlush: null
}

function ensureTemplates (context, templates, es) {
  const { debug, logger, url, indexPrefix } = context;
  const { username, password, validate_certs } = es;
  if (!validate_certs) { process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; }
  const templateNames = Object.keys(templates)
  return Promise.all(templateNames.map(templateName => {
    const template = templates[templateName];
    let fetchHeaders = {'Content-Type': 'application/json'};
    if (username && password) {
      fetchHeaders['Authorization'] = 'Basic ' + base64.encode(username+':'+password);
    }
    if (debug) {
      logger.log(`PUT url      = ${url}_template/${indexPrefix}${templateName.toLowerCase()}`)
      logger.log(`fetchHeaders = ${JSON.stringify(fetchHeaders)}`)
      logger.log(`body         = ${JSON.stringify(template)}`)
    }
    return fetch(`${url}_template/${indexPrefix}${templateName.toLowerCase()}`, {
      method: 'PUT',
      headers: fetchHeaders,
      body: JSON.stringify(template)
    }).then(resp => {
      if (resp.status !== 200) {
        return resp.json().then(data => {
          throw new Error(`Error creating template ${templateName}: HTTP ${resp.status}: ${JSON.stringify(data)}`)
        }, () => {
          throw new Error(`Error creating template ${templateName}: HTTP ${resp.status}`)
        })
      }
      return resp.json()
    })
  }))
}

function esBulkInsert (context, records, es) {
  const { debug, logger, url } = context;
  const { username, password, validate_certs } = es;

  if (!validate_certs) { process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"; }
  const body = records.map(i => JSON.stringify(i)).join('\n') + '\n'
  let fetchHeaders = {'Content-Type': 'application/json'};
  if (username && password) {
    fetchHeaders['Authorization'] = 'Basic ' + base64.encode(username+':'+password);
  }
  return fetch(url + '_bulk', {
    method: 'POST',
    headers: fetchHeaders,
    body
  })
    .then(resp => {
      if (resp.status !== 200) {
        return resp.json().then(data => {
          throw new Error(`Error inserting metrics: HTTP ${resp.status}: ${JSON.stringify(data)}`)
        }, () => {
          throw new Error(`Error inserting metrics: HTTP ${resp.status}`)
        })
      }
      return resp.json()
    })
    .then((data) => {
      elasticStats.inserts++
      if (debug) {
        logger.log(`Inserted ${records.length / 2} records into elasticsearch`)
      }
    }, error => {
      elasticStats.errors++
      logger.log(error.message, 'ERROR')
    })
}

const dateFormatters = {
  hour: (ms) => `${new Date(ms).toISOString().slice(0, 13)}`,
  day: (ms) => `${new Date(ms).toISOString().slice(0, 10)}`,
  month: (ms) => `${new Date(ms).toISOString().slice(0, 7)}`
}

const flushStats = (context, es) => (ts, metrics) => {
  const { logger, debug } = context
  const records = generateRecords(context, ts, metrics)

  if (debug) {
    logger.log('flushing ' + records.length / 2 + ' stats to ES')
  }
  esBulkInsert(context, records, es)
}

const generateRecords = (context, ts, metrics) => {
  const timeStamp = ts * 1000
  elasticStats.lastFlush = timeStamp
  const {
    indexPrefix,
    indexTimestamp,
    counterIndexName,
    timerIndexName,
    gaugeIndexName,
    setIndexName,
    hostname
  } = context

  const indexTimeStampValue = dateFormatters[indexTimestamp](timeStamp)

  const { counters, timer_counters: timerCounter, gauges, timer_data: timerData, sets } = metrics
  const records = []
  const queueRecord = (indexName, record) => {
    elasticStats.metricCount++
    records.push({
      index: {
        _index: `${indexPrefix}${indexName}-${indexTimeStampValue}`,
        _type: '_doc'
      }
    })
    records.push(record)
  }

  for (const metric of Object.keys(counters)) {
    queueRecord(counterIndexName, {
      '@timestamp': timeStamp,
      'src': hostname,
      metric,
      value: counters[metric]
    })
  }

  for (const metric of Object.keys(timerCounter)) {
    queueRecord(counterIndexName, {
      '@timestamp': timeStamp,
      'src': hostname,
      metric,
      value: timerCounter[metric]
    })
  }

  for (const metric of Object.keys(gauges)) {
    queueRecord(gaugeIndexName, {
      '@timestamp': timeStamp,
      'src': hostname,
      metric,
      value: gauges[metric]
    })
  }

  for (const metric of Object.keys(timerData)) {
    const record = Object.assign({
      '@timestamp': timeStamp,
      'src': hostname,
      metric
    }, timerData[metric])
    queueRecord(timerIndexName, record)
  }

  for (const metric of Object.keys(sets)) {
    queueRecord(setIndexName, {
      '@timestamp': timeStamp,
      'src': hostname,
      metric,
      value: sets[metric].store
    })
  }
  return records
}

const elasticBackendStatus = () => function (writeCb) {
  elasticStats.statusCalls++
  for (const stat of Object.keys(elasticStats)) {
    writeCb(null, 'elasticsearch', stat, elasticStats[stat])
  }
}

module.exports.init = function (startupTime, config, events, logger) {
  const { debug, elasticsearch } = config

  const {
    url = 'http://localhost:9200/',
    indexPrefix = 'statsd_',
    indexTimestamp = 'day', // month | hour
    counterIndexName = 'counter',
    timerIndexName = 'timer',
    gaugeIndexName = 'gauge',
    setIndexName = 'set',
    counterTemplate = templates.counterTemplate(`${indexPrefix}${counterIndexName}*`),
    timerTemplate = templates.timerTemplate(`${indexPrefix}${timerIndexName}*`),
    gaugeTemplate = templates.gaugeTemplate(`${indexPrefix}${gaugeIndexName}*`),
    setTemplate = templates.setTemplate(`${indexPrefix}${setIndexName}*`),
    shutdownOnStartupError = false,
    username,
    password,
    validate_certs
  } = elasticsearch || {}
  let hostname = os.hostname();

  const context = {
    debug,
    logger,
    url,
    indexPrefix,
    indexTimestamp,
    counterIndexName,
    timerIndexName,
    gaugeIndexName,
    setIndexName,
    hostname
  }

  if (debug) {
    logger.log('Elasticsearch backend loading', 'INFO')
  }
  ensureTemplates(context, { counterTemplate, timerTemplate, gaugeTemplate, setTemplate }, { username, password, validate_certs }).then(
    () => {
      logger.log('Elasticsearch templates loaded', 'INFO')
      events.on('flush', flushStats(context, elasticsearch))
      events.on('status', elasticBackendStatus(context))
    },
    err => {
      logger.log('Elasticsearch backend failure: unable to ensure index templates', 'ERROR')
      logger.log(err.message, 'ERROR')
      if (shutdownOnStartupError) {
        logger.log(`shutdownOnStartupError is true exiting`, 'ERROR')
        process.exit(1)
      }
    }
  )

  return true
}
