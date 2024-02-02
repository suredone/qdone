/**
 * Component to manage all the currently executing jobs, including extending
 * their visibility timeouts and deleting them when they are successful.
 */

import { ChangeMessageVisibilityBatchCommand, DeleteMessageBatchCommand } from '@aws-sdk/client-sqs'

import chalk from 'chalk'
import Debug from 'debug'

import { getSQSClient } from '../sqs.js'

const debug = Debug('qdone:jobExecutor')

const maxJobSeconds = 12 * 60 * 60

export class JobExecutor {
  constructor (opt) {
    this.opt = opt
    this.jobs = []
    this.jobsByMessageId = {}
    this.stats = {
      activeJobs: 0,
      waitingJobs: 0,
      runningJobs: 0,
      sqsCalls: 0,
      timeoutsExtended: 0,
      jobsSucceeded: 0,
      jobsFailed: 0,
      jobsDeleted: 0
    }
    this.maintainPromise = this.maintainVisibility()
    debug({ this: this })
  }

  async shutdown () {
    this.shutdownRequested = true
    // Trigger a maintenance run right away in case it speeds us up
    clearTimeout(this.maintainVisibilityTimeout)
    if (this.opt.verbose) {
      console.error(chalk.blue('Shutting down jobExecutor'))
    }
    await this.maintainPromise
    await this.maintainVisibility()
  }

  activeJobCount () {
    return this.stats.activeJobs
  }

  /**
   * Changes message visibility on all running jobs using as few calls as possible.
   */
  async maintainVisibility () {
    // Bail if we are shutting down
    if (this.shutdownRequested && this.stats.activeJobs === 0 && this.jobs.length === 0) {
      if (this.opt.verbose) {
        console.error(chalk.blue('All workers done, finishing shutdown of jobExecutor'))
      }
      return
    }

    // Reset our timeout
    clearTimeout(this.maintainVisibilityTimeout)
    const nextCheckInMs = this.shutdownRequested ? 1000 : 10 * 1000
    this.maintainVisibilityTimeout = setTimeout(() => {
      this.maintainPromise = this.maintainVisibility()
    }, nextCheckInMs)

    // debug('maintainVisibility', this.jobs)
    const start = new Date()
    const jobsToExtendByQrl = {}
    const jobsToDeleteByQrl = {}
    const jobsToCleanup = new Set()

    // Build list of jobs we need to deal with
    const jobStatuses = {}
    for (let i = 0; i < this.jobs.length; i++) {
      const job = this.jobs[i]
      const jobRunTime = Math.round((start - job.start) / 1000)
      jobStatuses[job.status] = (jobStatuses[job.status] || 0) + 1
      // debug('considering job', job)
      if (job.status === 'complete') {
        const jobsToDelete = jobsToDeleteByQrl[job.qrl] || []
        job.status = 'deleting'
        jobsToDelete.push(job)
        jobsToDeleteByQrl[job.qrl] = jobsToDelete
      } else if (job.status === 'failed') {
        jobsToCleanup.add(job)
      } else if (job.status !== 'deleting') {
        // Any other job state gets visibility accounting
        debug('processing', { job, jobRunTime })
        if (jobRunTime >= job.extendAtSecond) {
          // Add it to our organized list of jobs
          const jobsToExtend = jobsToExtendByQrl[job.qrl] || []
          jobsToExtend.push(job)
          jobsToExtendByQrl[job.qrl] = jobsToExtend

          // Update the visibility timeout, double every time, up to max
          const doubled = job.visibilityTimeout * 2
          const secondsUntilMax = Math.max(1, maxJobSeconds - jobRunTime)
          // const secondsUntilKill = Math.max(1, this.opt.killAfter - jobRunTime)
          job.visibilityTimeout = Math.min(doubled, secondsUntilMax) //, secondsUntilKill)
          job.extendAtSecond = Math.round(jobRunTime + job.visibilityTimeout / 2) // this is what we use next time
          debug({ doubled, secondsUntilMax, job })
        }
      }
    }

    if (this.opt.verbose) {
      console.error(chalk.blue('Stats: '), { stats: this.stats, jobStatuses })
      console.error(chalk.blue('Running: '), this.jobs.filter(j => j.status === 'processing').map(({ qname, message }) => ({ qname, payload: message.Body })))
    }

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
            Id: job.message.MessageId,
            ReceiptHandle: job.message.ReceiptHandle,
            VisibilityTimeout: job.visibilityTimeout
          }
          entries.push(entry)
        }
        debug({ entries })

        // Change batch
        const input = { QueueUrl: qrl, Entries: entries }
        debug({ ChangeMessageVisibilityBatch: input })
        const result = await getSQSClient().send(new ChangeMessageVisibilityBatchCommand(input))
        debug('ChangeMessageVisibilityBatch returned', result)
        this.stats.sqsCalls++
        if (result.Failed) {
          console.error('FAILED_MESSAGES', result.Failed)
          for (const failed of result.Failed) {
            console.error('FAILED_TO_EXTEND_JOB', this.jobsByMessageId[failed.Id])
          }
        }
        if (result.Successful) {
          const count = result.Successful.length || 0
          this.stats.timeoutsExtended += count
          if (this.opt.verbose) {
            console.error(chalk.blue('Extended'), count, chalk.blue('jobs'))
          } else if (!this.opt.disableLog) {
            console.log(JSON.stringify({ event: 'EXTEND_VISIBILITY_TIMEOUTS', timestamp: start, count, qrl }))
          }
        }
        // TODO Sentry
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
            Id: job.message.MessageId,
            ReceiptHandle: job.message.ReceiptHandle,
            VisibilityTimeout: job.visibilityTimeout
          }
          entries.push(entry)
        }
        debug({ entries })

        // Delete batch
        const input = { QueueUrl: qrl, Entries: entries }
        debug({ DeleteMessageBatch: input })
        const result = await getSQSClient().send(new DeleteMessageBatchCommand(input))
        this.stats.sqsCalls++
        if (result.Failed) {
          console.error('FAILED_MESSAGES', result.Failed)
          for (const failed of result.Failed) {
            console.error('FAILED_TO_DELETE_JOB', this.jobsByMessageId[failed.Id])
          }
        }
        if (result.Successful) {
          const count = result.Successful.length || 0
          this.stats.jobsDeleted += count
          if (this.opt.verbose) {
            console.error(chalk.blue('Deleted'), count, chalk.blue('jobs'))
          } else if (!this.opt.disableLog) {
            console.log(JSON.stringify({ event: 'DELETE_MESSAGES', timestamp: start, count, qrl }))
          }
        }
        debug('DeleteMessageBatch returned', result)
        // TODO Sentry
      }
    }

    // Get rid of deleted and failed jobs
    this.jobs = this.jobs.filter(job => {
      if (job.status === 'deleting' || job.status === 'failed') {
        debug('removed', job.message.MessageId)
        delete this.jobsByMessageId[job.message.MessageId]
        return false
      } else {
        return true
      }
    })
  }

  addJob (message, callback, qname, qrl) {
    // Create job entry and track it
    const defaultVisibilityTimeout = 60
    const job = {
      status: 'waiting',
      start: new Date(),
      visibilityTimeout: defaultVisibilityTimeout,
      extendAtSecond: defaultVisibilityTimeout / 2,
      payload: this.opt.json ? JSON.parse(message.Body) : message.Body,
      message,
      callback,
      qname,
      prettyQname: qname.slice(this.opt.prefix.length),
      qrl
    }

    // See if we are already executing this job
    const oldJob = this.jobsByMessageId[job.message.MessageId]
    if (oldJob) {
      // If we actually see the same job again, we fucked up, probably due to
      // the system being overloaded and us missing our extension call. So
      // we'll celebrate this occasion by throwing a big fat error.
      debug({ oldJob })
      const e = new Error(`Saw job ${oldJob.message.MessageId} twice`)
      e.job = oldJob
      // TODO: sentry breadcrumb
      throw e
    }

    this.jobs.push(job)
    this.jobsByMessageId[job.message.MessageId] = job
    this.stats.activeJobs++
    this.stats.waitingJobs++
    if (this.opt.verbose) {
      console.error(chalk.blue('Got message:'), job.prettyQname, chalk.blue('-->'), job.payload, job.message.MessageId)
    } else if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'MESSAGE_RECEIVED',
        timestamp: new Date(),
        queue: job.qname,
        messageId: message.MessageId,
        payload: job.payload
      }))
    }
    return job
  }

  async runJob (job) {
    try {
      if (this.opt.verbose) {
        console.error(chalk.blue('Running:'), job.prettyQname, chalk.blue('-->'), job.payload, job.message.MessageId)
      } else if (!this.opt.disableLog) {
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_START',
          timestamp: new Date(),
          queue: job.qname,
          messageId: job.message.MessageId,
          payload: job.payload
        }))
      }
      job.status = 'running'
      this.stats.runningJobs++
      this.stats.waitingJobs--
      const queue = job.qname.slice(this.opt.prefix.length)
      const result = await job.callback(queue, job.payload)
      debug('executeJob callback finished', { payload: job.payload, result })
      if (this.opt.verbose) {
        console.error(chalk.green('SUCCESS'), job.payload)
      }
      job.status = 'complete'

      if (this.opt.verbose) {
        console.error(chalk.blue('  done'))
        console.error()
      } else if (!this.opt.disableLog) {
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_COMPLETE',
          queue: job.qname,
          timestamp: new Date(),
          messageId: job.message.MessageId,
          payload: job.payload
        }))
      }
      this.stats.jobsSucceeded++
    } catch (err) {
      job.status = 'failed'
      this.stats.jobsFailed++
      // Fail path for job execution
      if (this.opt.verbose) {
        console.error(chalk.red('FAILED'), job.payload)
        console.error(chalk.blue('  error : ') + err)
      } else if (!this.opt.disableLog) {
        // Production error logging
        console.log(JSON.stringify({
          event: 'MESSAGE_PROCESSING_FAILED',
          reason: 'exception thrown',
          queue: job.qname,
          timestamp: new Date(),
          messageId: job.message.MessageId,
          payload: job.payload,
          errorMessage: err.toString().split('\n').slice(1).join('\n').trim() || undefined,
          err
        }))
      }
    }
    this.stats.activeJobs--
    this.stats.runningJobs--
  }

  async executeJobs (messages, callback, qname, qrl) {
    if (this.shutdownRequested) throw new Error('jobExecutor is shutting down so cannot execute new jobs')

    // Begin tracking jobs
    const jobs = messages.map(message => this.addJob(message, callback, qname, qrl))
    const isFifo = qrl.endsWith('.fifo')

    // console.log(jobs)

    // Begin executing
    for (const [job, i] of jobs.map((job, i) => [job, i])) {
      // Figure out if the next job needs to happen in serial, otherwise we can parallel execute
      // const job = jobs[i]
      const nextJob = jobs[i + 1]
      const nextJobIsSerial =
         isFifo && nextJob &&
         job.message?.Attributes?.GroupId === nextJob.message?.Attributes?.GroupId

      console.log({ i, nextJobAtt: nextJob.message.Attributes, nextJobIsSerial })
      // Execute serial or parallel
      if (nextJobIsSerial) await this.runJob(job)
      else this.runJob(job)
    }
  }
}
