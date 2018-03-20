
const debug = require('debug')('qdone:idleQueues')
const chalk = require('chalk')
const qrlCache = require('./qrlCache')
const AWS = require('aws-sdk')

// Queue attributes we check to determine idle
const attributeNames = [
  'ApproximateNumberOfMessages',
  'ApproximateNumberOfMessagesNotVisible',
  'ApproximateNumberOfMessagesDelayed'
]

// CloudWatch metrics we check to determine idle
const metricNames = [
  'NumberOfMessagesSent',
  'NumberOfMessagesReceived',
  'NumberOfMessagesDeleted',
  'ApproximateNumberOfMessagesVisible',
  'ApproximateNumberOfMessagesNotVisible',
  'ApproximateNumberOfMessagesDelayed',
  //    'NumberOfEmptyReceives',
  'ApproximateAgeOfOldestMessage'
]

/**
 * Gets queue attributes from the SQS api and assesses whether queue is idle
 * at this immediate moment.
 */
function cheapIdleCheck (qname, qrl, options) {
  const sqs = new AWS.SQS()
  return sqs
    .getQueueAttributes({AttributeNames: attributeNames, QueueUrl: qrl})
    .promise()
    .then(data => {
      // debug('data', data)
      const result = data.Attributes
      result.queue = qname.slice(options.prefix.length)
      result.idle = attributeNames.filter(k => result[k] === '0').length === attributeNames.length
      return Promise.resolve(result)
    })
}

/**
 * Gets a single metric from the CloudWatch api.
 */
function getMetric (qname, qrl, metricName, options) {
  debug('getMetric', qname, qrl, metricName)
  const now = new Date()
  const params = {
    StartTime: new Date(now.getTime() - 1000 * 60 * options['idle-for']),
    EndTime: now,
    MetricName: metricName,
    Namespace: 'AWS/SQS',
    Period: 3600,
    Dimensions: [{Name: 'QueueName', Value: qname}],
    Statistics: ['Sum']
    // Unit: ['']
  }
  const cloudwatch = new AWS.CloudWatch()
  return cloudwatch
    .getMetricStatistics(params)
    .promise()
    .then(data => {
      debug('getMetric data', data)
      return Promise.resolve({
        [metricName]: data.Datapoints.map(d => d.Sum).reduce((a, b) => a + b, 0)
      })
    })
}

/**
 * Checks if a single queue is idle. First queries the cheap ($0.40/1M) SQS API and
 * continues to the expensive ($10/1M) CloudWatch API, checking to make sure there
 * were no messages in the queue in the specified time period. Only if all relevant
 * metrics show no messages, is it ok to delete the queue.
 *
 * Realistically, the number of CloudWatch calls will depend on usage patterns, but
 * I see a ~70% reduction in calls by checking metrics in the given order.
 * We could randomize the order, but for my test use case, it's always cheaper
 * to check NumberOfMessagesSent first, and is the primary indicator of use.
 */
function checkIdle (qname, qrl, options) {
  // Do the cheap check first to make sure there is no data in flight at the moment
  debug('checkIdle', qname, qrl)
  return cheapIdleCheck(qname, qrl, options)
    .then(cheapResult => {
      debug('cheapResult', cheapResult)
      // Short circuit further calls if cheap result shows data
      if (cheapResult.idle === false) {
        return {
          queue: qname.slice(options.prefix.length),
          cheap: cheapResult,
          idle: false,
          apiCalls: {SQS: 1, CloudWatch: 0}
        }
      }
      // If we get here, there's nothing in the queue at the moment,
      // so we have to check metrics one at a time
      return metricNames.reduce((promiseChain, metricName) => {
        return promiseChain.then((soFar = {
          queue: qname.slice(options.prefix.length),
          cheap: cheapResult,
          idle: true,
          apiCalls: {SQS: 1, CloudWatch: 0}
        }) => {
          debug('soFar', soFar)
          // Break out of our call chain if we find one failed check
          if (soFar.idle === false) return Promise.resolve(soFar)
          return getMetric(qname, qrl, metricName, options)
            .then(result => {
              debug('getMetric result', result)
              return Object.assign(
                soFar, // start with soFar object
                result, // add in our metricName keyed result
                {idle: result[metricName] === 0}, // and recalculate idle
                {apiCalls: {
                  SQS: soFar.apiCalls.SQS,
                  CloudWatch: soFar.apiCalls.CloudWatch + 1
                }}
              )
            })
        })
      }, Promise.resolve())
    })
}

/**
 * Just deletes a queue.
 */
function deleteQueue (qname, qrl, options) {
  const sqs = new AWS.SQS()
  return sqs.deleteQueue({QueueUrl: qrl})
    .promise()
    .then(result => {
      debug(result)
      if (options.verbose) console.error(chalk.blue('Deleted ') + qname.slice(options.prefix.length))
      return Promise.resolve({
        deleted: true,
        apiCalls: {SQS: 1, CloudWatch: 0}
      })
    })
}

/**
 * Processes a single queue, checking for idle, deleting if applicable.
 */
function processQueue (qname, qrl, options) {
  return checkIdle(qname, qrl, options)
    .then(result => {
      debug(qname, result)
      if (result.idle) {
        if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))
        // Trigger a delete if the user wants it
        if (options.delete) {
          // End this branch of the tree
          return deleteQueue(qname, qrl, options)
            .then(deleteResult => Object.assign(result, {
              deleted: deleteResult.deleted,
              apiCalls: {
                SQS: result.apiCalls.SQS + deleteResult.apiCalls.SQS,
                CloudWatch: result.apiCalls.CloudWatch + deleteResult.apiCalls.CloudWatch
              }
            }))
        }
      } else {
        if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
      }
      // End this branch of the tree
      return Promise.resolve(result)
    })
}

/**
 * Processes a queue and its fail queue, treating them as a unit.
 */
function processQueuePair (qname, qrl, options) {
  const fqname = qname + options['fail-suffix']
  const fqrl = qrl + options['fail-suffix']
  return checkIdle(qname, qrl, options).then(result => {
    debug('result', result)
    if (result.idle) {
      if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))
      // Check fail queue if we get a positive result for normal queue
      return checkIdle(fqname, fqrl, options).then(fresult => {
        debug('fresult', fresult)
        if (fresult.idle) {
          if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' has been ') + 'idle' + chalk.blue(' for the last ') + options['idle-for'] + chalk.blue(' minutes.'))
          // Trigger a delete if the user wants it
          if (options.delete) {
            // End this branch of the tree
            return Promise.all([
              deleteQueue(qname, qrl, options),
              deleteQueue(fqname, fqrl, options)
            ]).then(deleteResults =>
              deleteResults.reduce((a, b) => Object.assign(a, {apiCalls: {
                SQS: a.apiCalls.SQS + b.apiCalls.SQS,
                CloudWatch: a.apiCalls.CloudWatch + b.apiCalls.CloudWatch
              }}), Object.assign(result, {failq: fresult}, {apiCalls: {
                SQS: result.apiCalls.SQS + fresult.apiCalls.SQS,
                CloudWatch: result.apiCalls.CloudWatch + fresult.apiCalls.CloudWatch
              }}))
            )
          }
        } else {
          if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
        }
        return Promise.resolve(Object.assign(result, {idle: result.idle && fresult.idle, failq: fresult}, {apiCalls: {
          SQS: result.apiCalls.SQS + fresult.apiCalls.SQS,
          CloudWatch: result.apiCalls.CloudWatch + fresult.apiCalls.CloudWatch
        }}))
      })
        .catch(e => {
        // Handle the case where the fail queue has been deleted or was never created for some reason
          if (e.code === 'AWS.SimpleQueueService.NonExistentQueue') {
            if (options.verbose) console.error(chalk.blue('Queue ') + fqname.slice(options.prefix.length) + chalk.blue(' does not exist.'))
            if (options.delete) {
              return deleteQueue(qname, qrl, options)
                .then(deleteResult => Object.assign(result, {
                  deleted: deleteResult.deleted,
                  apiCalls: {
                    SQS: result.apiCalls.SQS + deleteResult.apiCalls.SQS,
                    CloudWatch: result.apiCalls.CloudWatch + deleteResult.apiCalls.CloudWatch
                  }
                }))
            } else {
              return result
            }
          } else {
            throw e
          }
        })
    } else {
      if (options.verbose) console.error(chalk.blue('Queue ') + qname.slice(options.prefix.length) + chalk.blue(' has been ') + 'active' + chalk.blue(' in the last ') + options['idle-for'] + chalk.blue(' minutes.'))
    }
    // End this branch of the tree
    return result
  })
}

//
// Resolve queues for listening loop listen
//
exports.idleQueues = function idleQueues (queues, options) {
  if (options.verbose) console.error(chalk.blue('Resolving queues: ') + queues.join(' '))
  const qnames = queues.map(function (queue) { return options.prefix + queue })
  return qrlCache
    .getQnameUrlPairs(qnames, options)
    .then(function (entries) {
      debug('qrlCache.getQnameUrlPairs.then')
      if (options.verbose) {
        console.error(chalk.blue('  done'))
        console.error()
      }

      // Filter out any queue ending in suffix unless --include-failed is set
      entries = entries
        .filter(function (entry) {
          const suf = options['fail-suffix']
          return options['include-failed'] ? true : entry.qname.slice(-suf.length) !== suf
        })

      // But only if we have queues to remove
      if (entries.length) {
        if (options.verbose) {
          console.error(chalk.blue('Checking queues (in this order):'))
          console.error(entries.map(e =>
            '  ' + e.qname.slice(options.prefix.length) + chalk.blue(' - ' + e.qrl)
          ).join('\n'))
          console.error()
        }
        // Check each queue in parallel
        if (options.unpair) return Promise.all(entries.map(e => processQueue(e.qname, e.qrl, options)))
        return Promise.all(entries.map(e => processQueuePair(e.qname, e.qrl, options)))
      }

      // Otherwise, let caller know
      return Promise.resolve('noQueues')
    })
}

debug('loaded')
