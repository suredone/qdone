/**
 * Component to manage all the currently executing jobs, including extending
 * their visibility timeouts and deleting them when they are successful.
 */

import {
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand
} from '@aws-sdk/client-sqs'
import chalk from 'chalk'
import Debug from 'debug'

import { getSQSClient } from '../sqs.js'

const debug = Debug('qdone:jobExecutor')

const maxJobSeconds = 12 * 60 * 60

export class JobExecutor {
  constructor (opt) {
    this.opt = opt
    this.jobs = []
    this.stats = {
      activeJobs: 0,
      sqsCalls: 0,
      timeoutsExtended: 0,
      jobsSucceeded: 0,
      jobsFailed: 0,
      jobsDeleted: 0
    }
    this.maintainVisibility()
    debug({ this: this })
  }

  shutdown () {
    this.shutdownRequested = true
    if (this.stats.activeJobs === 0 && this.jobs.length === 0) {
      clearTimeout(this.maintainVisibilityTimeout)
    }
  }

  activeJobCount () {
    return this.stats.activeJobs
  }

  /**
   * Changes message visibility on all running jobs using as few calls as possible.
   */
  async maintainVisibility () {
    debug('maintainVisibility', this.jobs)
    const now = new Date()
    const jobsToExtendByQrl = {}
    const jobsToDeleteByQrl = {}
    const jobsToCleanup = new Set()

    if (this.opt.verbose) {
      console.error(chalk.blue('Stats: '), this.stats)
      console.error(chalk.blue('Running: '), this.jobs.filter(j => j.status === 'processing').map(({ qname, message }) => ({ qname, payload: message.Body })))
    }

    // Build list of jobs we need to deal with
    for (const job of this.jobs) {
      const jobRunTime = (now - job.start) / 1000
      if (job.status === 'complete') {
        const jobsToDelete = jobsToDeleteByQrl[job.qrl] || []
        jobsToDelete.push(job)
        jobsToDeleteByQrl[job.qrl] = jobsToDelete
      } else if (job.status === 'failed') {
        jobsToCleanup.add(job)
      } else if (jobRunTime >= job.exendAtSecond) {
        // Add it to our organized list of jobs
        const jobsToExtend = jobsToExtendByQrl[job.qrl] || []
        jobsToExtend.push(job)
        jobsToExtendByQrl[job.qrl] = jobsToExtend

        // Update the visibility timeout, double every time, up to max
        const doubled = job.visibilityTimeout * 2
        const secondsUntilMax = maxJobSeconds - jobRunTime
        const secondsUntilKill = this.opt.killAfter - jobRunTime
        job.visibilityTimeout = Math.min(double, secondsUntilMax, secondsUntilKill)
        job.extendAtSecond = jobRunTime + job.visibilityTimeout // this is what we use next time
      }
    }
    debug('maintainVisibility', { jobsToDeleteByQrl, jobsToExtendByQrl })

    // Extend in batches for each queue
    for (const qrl in jobsToExtendByQrl) {
      const jobsToExtend = jobsToExtendByQrl[qrl]
      debug({ qrl, jobsToExtend })
      while (jobsToExtend.length) {
        // Build list of messages to go in this batch
        const entries = []
        let messageId = 0
        while (messageId++ < 10 && jobsToExtend.length) {
          const job = jobsToExtend.shift()
          const entry = {
            Id: '' + messageId,
            ReceiptHandle: job.message.ReceiptHandle,
            VisibilityTimeout: job.visibilityTimeout
          }
          entries.push(entry)
        }

        // Change batch
        const input = { QueueUrl: qrl, Entries: entries }
        debug({ ChangeMessageVisibilityBatch: input })
        const result = await getSQSClient().send(new ChangeMessageVisibilityBatchCommand(input))
        debug('ChangeMessageVisibilityBatch returned', result)
        this.stats.sqsCalls++
        this.stats.timeoutsExtended += 10
        // TODO Sentry
      }
      if (this.opt.verbose) {
        console.error(chalk.blue('Extended these jobs: '), jobsToExtend)
      } else if (!this.opt.disableLog) {
        console.log(JSON.stringify({
          event: 'EXTEND_VISIBILITY_TIMEOUTS',
          timestamp: now,
          messageIds: jobsToExtend.map(({ message }) => message.MessageId)
        }))
      }
    }

    // Delete in batches for each queue
    for (const qrl in jobsToDeleteByQrl) {
      const jobsToDelete = jobsToDeleteByQrl[qrl]
      while (jobsToDelete.length) {
        // Build list of messages to go in this batch
        const entries = []
        let messageId = 0
        while (messageId++ < 10 && jobsToDelete.length) {
          const job = jobsToDelete.shift()
          const entry = {
            Id: '' + messageId,
            ReceiptHandle: job.message.ReceiptHandle,
            VisibilityTimeout: job.visibilityTimeout
          }
          entries.push(entry)
        }

        // Delete batch
        const input = { QueueUrl: qrl, Entries: entries }
        debug({ DeleteMessageBatch: input })
        const result = await getSQSClient().send(new DeleteMessageBatchCommand(input))
        this.stats.sqsCalls++
        this.stats.jobsDeleted += 10
        debug('DeleteMessageBatch returned', result)
        // TODO Sentry
      }
      if (this.opt.verbose) {
        console.error(chalk.blue('Deleted these finished jobs: '), jobsToDelete)
      } else if (!this.opt.disableLog) {
        console.log(JSON.stringify({
          event: 'DELETE_MESSAGES',
          timestamp: now,
          messageIds: jobsToDelete.map(({ message }) => message.MessageId)
        }))
      }
    }

    // Get rid of deleted and failed jobs
    this.jobs = this.jobs.filter(j => j.status === 'processing')

    // Check again later, unless we are shutting down and nothing's left
    if (this.shutdownRequested && this.stats.activeJobs === 0 && this.jobs.length === 0) return
    this.maintainVisibilityTimeout = setTimeout(() => this.maintainVisibility(), 10 * 1000)
  }

  async executeJob (message, callback, qname, qrl) {
    // Create job entry and track it
    const payload = this.opt.json ? JSON.parse(message.Body) : message.Body
    const visibilityTimeout = 30
    const job = {
      status: 'processing',
      start: new Date(),
      visibilityTimeout: 30,
      extendAtSecond: visibilityTimeout / 2,
      payload: this.opt.json ? JSON.parse(message.Body) : message.Body,
      message,
      callback,
      qname,
      qrl
    }
    debug('executeJob', job)
    this.jobs.push(job)
    this.stats.activeJobs++
    if (this.opt.verbose) {
      console.error(chalk.blue('Executing:'), qname, chalk.blue('-->'), job.payload)
    } else if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'MESSAGE_PROCESSING_START',
        timestamp: new Date(),
        qname,
        messageId: message.MessageId,
        payload: job.payload
      }))
    }

    // Execute job
    try {
      const queue = qname.slice(this.opt.prefix.length)
      const result = await callback(queue, payload)
      debug('executeJob callback finished', { payload, result })
      if (this.opt.verbose) {
        console.error(chalk.green('SUCCESS'), message.Body)
      }
      job.status = 'complete'

      if (this.opt.verbose) {
        console.error(chalk.blue('  done'))
        console.error()
      } else if (!this.opt.disableLog) {
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_COMPLETE',
          timestamp: new Date(),
          messageId: message.MessageId,
          payload
        }))
      }
      this.stats.jobsSucceeded++
    } catch (err) {
      debug('exec.catch')
      // Fail path for job execution
      if (this.opt.verbose) {
        console.error(chalk.red('FAILED'), message.Body)
        console.error(chalk.blue('  error : ') + err)
      } else if (!this.opt.disableLog) {
        // Production error logging
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_FAILED',
          reason: 'exception thrown',
          qname,
          timestamp: new Date(),
          messageId: message.MessageId,
          payload,
          errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined,
          err
        }))
      }
      job.status = 'failed'
      this.stats.jobsFailed++
    }
    this.stats.activeJobs--
  }
}
