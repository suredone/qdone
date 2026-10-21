/**
 * Component to manage all the currently executing jobs, including extending
 * their visibility timeouts and deleting them when they are successful.
 */

import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand
} from '@aws-sdk/client-sqs'

import chalk from 'chalk'
import Debug from 'debug'
import treeKill from 'tree-kill'

import { dedupSuccessfullyProcessed } from '../dedup.js'
import { getSQSMaintenanceClient } from '../sqs.js'

const debug = Debug('qdone:jobExecutor')

const maxJobSeconds = 12 * 60 * 60
const defaultVisibilityTimeout = 120
const SIGKILL_DELAY_MS = 5000
// Completions stop starting early passes while this many are in flight; the 10s timer still runs.
const maxPromptMaintenancePasses = 3

export class JobExecutor {
  constructor (opt) {
    this.opt = opt
    this.jobs = [] // for full traversals
    this.jobsByMessageId = {} // for looking up via message id
    this.jobsByQueue = new Map() // for looking up via queue name
    this.stats = {
      activeJobs: 0,
      waitingJobs: 0,
      runningJobs: 0,
      sqsCalls: 0,
      timeoutsExtended: 0,
      jobsSucceeded: 0,
      jobsFailed: 0,
      jobsDeleted: 0,
      jobsKilled: 0
    }
    this.executionPromises = new Set()
    this.killEscalationPromises = new Set()
    this.maintainPromises = new Set()
    this.maintainVisibility()
    debug({ this: this })
  }

  async shutdown () {
    this.shutdownRequested = true
    clearTimeout(this.maintainVisibilityTimeout)
    if (this.opt.verbose) {
      console.error(chalk.blue('Shutting down jobExecutor'))
    }

    // Finish every maintenance pass already in progress, then wait for every batch
    // accepted before shutdown. executeJobs encompasses FIFO queue waits as well
    // as callbacks, so active work cannot outlive this method. Passes may overlap,
    // and one that fails must not leave another's delete running when the clients close.
    const maintenanceResults = await Promise.allSettled([...this.maintainPromises])
    const executionResults = await Promise.allSettled([...this.executionPromises])
    await Promise.allSettled([...this.killEscalationPromises])

    // Delete completed messages and remove failed jobs before shared SQS/cache
    // clients are closed by the consumer.
    clearTimeout(this.maintainVisibilityTimeout)
    maintenanceResults.push(...await Promise.allSettled([this.maintainVisibility()]))
    clearTimeout(this.maintainVisibilityTimeout)
    maintenanceResults.push(...await Promise.allSettled([...this.maintainPromises]))

    const failures = [...executionResults, ...maintenanceResults]
      .filter(result => result.status === 'rejected')
      .map(result => result.reason)
    if (this.stats.activeJobs !== 0 || this.jobs.length !== 0) {
      const invariantError = new Error(`jobExecutor shutdown invariant failed: ${JSON.stringify(this.shutdownStatus())}`)
      // A pass that failed before its cleanup leaves jobs tracked; report why alongside the invariant.
      if (failures.length) {
        throw new AggregateError([invariantError, ...failures], 'One or more qdone batches failed during shutdown')
      }
      throw invariantError
    }

    if (failures.length) {
      throw new AggregateError(failures, 'One or more qdone batches failed during shutdown')
    }
  }

  shutdownStatus () {
    const now = new Date()
    const longestRunningMs = this.jobs.reduce((longest, job) => {
      if (!job.executionStart || job.status !== 'running') return longest
      return Math.max(longest, now - job.executionStart)
    }, 0)

    return {
      activeJobs: this.stats.activeJobs,
      waitingJobs: this.stats.waitingJobs,
      runningJobs: this.stats.runningJobs,
      trackedBatches: this.executionPromises.size,
      pendingKillEscalations: this.killEscalationPromises.size,
      maintenancePasses: this.maintainPromises.size,
      trackedJobs: this.jobs.length,
      longestRunningMs
    }
  }

  activeJobCount () {
    return this.stats.activeJobs
  }

  runningJobCount () {
    return this.stats.runningJobs
  }

  /**
   * Returns the number of jobs running in a queue.
   */
  runningJobCountForQueue (qname) {
    const jobs = this.jobsByQueue.get(qname) || new Set()
    let runningCount = 0
    for (const job of jobs.values()) runningCount += job.status === 'running'
    return runningCount
  }

  clearJobTimers (job) {
    clearTimeout(job.killTimer)
  }

  cancelKillEscalation (job) {
    clearTimeout(job.killSignalTimer)
    job.resolveKillEscalation?.()
  }

  trackKillEscalation (escalation) {
    this.killEscalationPromises.add(escalation)
    const removeEscalation = () => this.killEscalationPromises.delete(escalation)
    escalation.then(removeEscalation, removeEscalation)
    return escalation
  }

  scheduleKillEscalation (job, signal) {
    this.cancelKillEscalation(job)
    const escalation = new Promise(resolve => {
      job.resolveKillEscalation = resolve
      job.killSignalTimer = setTimeout(() => {
        try {
          signal()
        } finally {
          resolve()
        }
      }, SIGKILL_DELAY_MS)
    })
    job.killEscalationPromise = escalation
    this.trackKillEscalation(escalation)
    escalation.finally(() => {
      if (job.killEscalationPromise === escalation) {
        delete job.killEscalationPromise
        delete job.resolveKillEscalation
        delete job.killSignalTimer
      }
    })
  }

  getExecutionTimeMs (job, start = new Date()) {
    return start - job.executionStart
  }

  shouldEnforceKillAfter (job) {
    return !!(this.opt.killAfter && job.executionMode !== 'inline')
  }

  scheduleKillAfter (job) {
    if (!this.opt.killAfter) return
    clearTimeout(job.killTimer)
    job.killTimer = setTimeout(() => {
      job.killDue = true
      this.killJob(job, new Date())
    }, this.opt.killAfter * 1000)
    job.killTimer.unref?.()
  }

  signalProcessGroup (job, signal) {
    const killProcessGroup = this.opt.killProcessGroup || ((pid, signal) => process.kill(-pid, signal))
    try {
      killProcessGroup(job.pid, signal)
      return { signal, succeeded: true }
    } catch (err) {
      if (err.code === 'ESRCH') return { signal, succeeded: true, alreadyExited: true }
      debug(`process group ${signal} error`, err.message)
      return { signal, succeeded: false, error: err.message }
    }
  }

  killJob (job, start = new Date()) {
    if (!job.executionStart || job.status !== 'running') return
    if (job.killed) return
    if (!this.shouldEnforceKillAfter(job)) return

    const executionTimeMs = this.getExecutionTimeMs(job, start)
    if (executionTimeMs < this.opt.killAfter * 1000) return
    const executionTime = Math.floor(executionTimeMs / 1000)

    job.killDue = true
    if (!job.pid) {
      debug('killAfter reached before PID registration', { messageId: job.message?.MessageId, executionTime })
      return
    }

    job.killed = true
    this.stats.jobsKilled++
    const pid = job.pid
    const killTree = this.opt.killTree || treeKill

    if (this.opt.verbose) {
      console.error(chalk.red('KILLING'), job.prettyQname, chalk.red('pid'), pid,
        chalk.red('after'), executionTime, chalk.red('seconds (limit:'), this.opt.killAfter + ')')
    } else if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'JOB_KILL_AFTER',
        timestamp: start,
        queue: job.qname,
        messageId: job.message.MessageId,
        pid,
        executionTime,
        killAfter: this.opt.killAfter,
        payload: job.payload
      }))
    }

    if (job.processGroup) {
      this.signalProcessGroup(job, 'SIGTERM')
    } else {
      killTree(pid, 'SIGTERM', (err) => {
        if (err) debug('treeKill SIGTERM error', err.message)
      })
    }

    // Keep the escalation independent of the root callback lifecycle: the
    // process leader may exit on SIGTERM while a descendant in its group does
    // not. shutdown() tracks this promise so the worker cannot finish draining
    // before the group-wide SIGKILL has been attempted.
    this.scheduleKillEscalation(job, () => {
      if (job.processGroup) {
        this.signalProcessGroup(job, 'SIGKILL')
        return
      }
      try { process.kill(pid, 0) } catch (e) { if (e.code === 'ESRCH') return }
      killTree(pid, 'SIGKILL', (err) => {
        if (err) debug('treeKill SIGKILL error', err.message)
      })
    })
  }

  async signalJobTree (killTree, pid, signal) {
    return new Promise(resolve => {
      const timeout = setTimeout(() => {
        debug(`treeKill ${signal} callback timed out`, { pid })
        resolve({ signal, succeeded: false, timedOut: true })
      }, this.opt.forceSignalTimeoutMs ?? 5000)
      const finish = (result) => {
        clearTimeout(timeout)
        resolve(result)
      }
      try {
        killTree(pid, signal, (err) => {
          if (err) debug(`treeKill ${signal} error`, err.message)
          finish({ signal, succeeded: !err, ...(err ? { error: err.message } : {}) })
        })
      } catch (err) {
        debug(`treeKill ${signal} threw`, err.message)
        finish({ signal, succeeded: false, error: err.message })
      }
    })
  }

  async forceTerminateJob (job, start = new Date()) {
    if (!job.executionStart || job.status !== 'running' || !job.pid || job.forceTerminating) return false

    job.forceTerminating = true
    job.killDue = true
    if (!job.killed) {
      job.killed = true
      this.stats.jobsKilled++
    }
    clearTimeout(job.killTimer)
    this.cancelKillEscalation(job)

    const pid = job.pid
    const executionTime = Math.floor(this.getExecutionTimeMs(job, start) / 1000)
    const killTree = this.opt.killTree || treeKill
    if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'JOB_FORCE_TERMINATED',
        timestamp: start,
        queue: job.qname,
        messageId: job.message.MessageId,
        pid,
        executionTime,
        killAfter: this.opt.killAfter,
        payload: job.payload
      }))
    }

    const signals = []
    signals.push(job.processGroup
      ? this.signalProcessGroup(job, 'SIGTERM')
      : await this.signalJobTree(killTree, pid, 'SIGTERM'))
    const forceKillDelayMs = this.opt.forceKillDelayMs ?? 1000
    await new Promise(resolve => setTimeout(resolve, forceKillDelayMs))
    signals.push(job.processGroup
      ? this.signalProcessGroup(job, 'SIGKILL')
      : await this.signalJobTree(killTree, pid, 'SIGKILL'))
    return {
      messageId: job.message.MessageId,
      pid,
      processGroup: !!job.processGroup,
      signals
    }
  }

  forceTerminateActiveJobs () {
    // Register the aggregate synchronously. TERM can settle a callback before
    // the first await below resumes, and an already-running shutdown must still
    // own the grace period and group-wide SIGKILL attempt.
    return this.trackKillEscalation(this.forceTerminateActiveJobsInternal())
  }

  async forceTerminateActiveJobsInternal () {
    const start = new Date()
    const results = await Promise.all(this.jobs.map(job => this.forceTerminateJob(job, start)))
    const jobs = results.filter(Boolean)
    return {
      attemptedJobs: jobs.length,
      failedSignals: jobs.flatMap(job => job.signals.filter(signal => !signal.succeeded)),
      jobs
    }
  }

  async setJobVisibilityTimeout (job, visibilityTimeout, start = new Date()) {
    if (job.extendAtSecond === Infinity) return

    job.visibilityTimeout = visibilityTimeout
    const jobRunTime = Math.round((start - job.start) / 1000)
    job.extendAtSecond = Math.round(jobRunTime + job.visibilityTimeout / 2)

    const input = {
      QueueUrl: job.qrl,
      ReceiptHandle: job.message.ReceiptHandle,
      VisibilityTimeout: job.visibilityTimeout
    }
    debug({ ChangeMessageVisibility: input })

    try {
      const result = await getSQSMaintenanceClient().send(new ChangeMessageVisibilityCommand(input))
      debug('ChangeMessageVisibility returned', result)
      this.stats.sqsCalls++
      this.stats.timeoutsExtended++
    } catch (err) {
      debug('ChangeMessageVisibility error', err)
      if (this.opt.verbose) {
        console.error(chalk.red('FAILED_TO_SET_VISIBILITY_TIMEOUT'), { err, input })
      }
    }
  }

  async setRunningVisibilityTimeout (job) {
    if (!this.shouldEnforceKillAfter(job)) return

    const visibilityTimeout = Math.max(1, Math.min(job.visibilityTimeout, this.opt.killAfter))
    if (visibilityTimeout >= job.visibilityTimeout) return

    await this.setJobVisibilityTimeout(job, visibilityTimeout)
  }

  async registerInlineExecution (job) {
    if (job.executionMode === 'inline') return
    if (job.executionMode === 'child_process') {
      debug('registerInlineExecution ignored after registerPid', { messageId: job.message?.MessageId })
      return
    }

    job.executionMode = 'inline'
    job.killDue = false
    this.clearJobTimers(job)

    if (job.status === 'running' && job.visibilityTimeout < defaultVisibilityTimeout) {
      await this.setJobVisibilityTimeout(job, defaultVisibilityTimeout)
    }
  }

  logInlineKillAfterOverrun (job, start = new Date()) {
    if (!this.opt.killAfter || !job.executionStart || job.inlineKillAfterLogged) return

    const executionTimeMs = this.getExecutionTimeMs(job, start)
    if (executionTimeMs < this.opt.killAfter * 1000) return

    job.inlineKillAfterLogged = true
    const executionTime = Math.floor(executionTimeMs / 1000)
    if (this.opt.verbose) {
      console.error(chalk.yellow('INLINE_JOB_EXCEEDED_KILL_AFTER'), job.prettyQname,
        chalk.yellow('after'), executionTime, chalk.yellow('seconds (limit:'), this.opt.killAfter + ')')
    } else if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'INLINE_JOB_EXCEEDED_KILL_AFTER',
        timestamp: start,
        queue: job.qname,
        messageId: job.message.MessageId,
        executionTime,
        killAfter: this.opt.killAfter,
        payload: job.payload
      }))
    }
  }

  scheduleMaintenance (delayMs) {
    clearTimeout(this.maintainVisibilityTimeout)
    this.maintainVisibilityTimeout = setTimeout(() => {
      this.maintainVisibility()
    }, delayMs)
  }

  // Passes may overlap, so a stuck delete never holds up another pass's extensions;
  // shutdown waits for every one before the clients close. A failed pass rejects once:
  // to shutdown when it is waiting, otherwise as an unhandled rejection.
  maintainVisibility () {
    const pass = this.maintainVisibilityPass().finally(() => this.maintainPromises.delete(pass))
    this.maintainPromises.add(pass)
    return pass
  }

  /**
   * Changes message visibility on all running jobs using as few calls as possible.
   */
  async maintainVisibilityPass () {
    // Bail if we are shutting down
    if (this.shutdownRequested && this.stats.activeJobs === 0 && this.jobs.length === 0) {
      if (this.opt.verbose) {
        console.error(chalk.blue('All workers done, finishing shutdown of jobExecutor'))
      }
      return
    }

    // Reset our timeout
    this.scheduleMaintenance(this.shutdownRequested ? 1000 : 10 * 1000)

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

        // Kill-after enforcement: terminate child process if it exceeds the deadline.
        // Uses executionStart (when runJob began) so FIFO serial jobs aren't
        // penalized for queue wait time.
        if (this.shouldEnforceKillAfter(job) && job.executionStart && !job.killed) {
          const executionTimeMs = this.getExecutionTimeMs(job, start)
          if (executionTimeMs >= this.opt.killAfter * 1000) {
            job.killDue = true
            this.killJob(job, start)
          }
        } else if (job.executionMode === 'inline') {
          this.logInlineKillAfterOverrun(job, start)
        }

        if (jobRunTime >= job.extendAtSecond) {
          // Add it to our organized list of jobs
          const jobsToExtend = jobsToExtendByQrl[job.qrl] || []
          jobsToExtendByQrl[job.qrl] = jobsToExtend

          // Update the visibility timeout, double every time, up to max.
          // Only cap at killAfter once execution has started — waiting FIFO
          // jobs should not have their visibility reduced prematurely.
          const previousVisibilityTimeout = job.visibilityTimeout
          const doubled = previousVisibilityTimeout * 2
          const secondsUntilMax = Math.max(1, maxJobSeconds - jobRunTime)
          const executionTimeMs = job.executionStart ? this.getExecutionTimeMs(job, start) : 0
          const secondsUntilKill = (this.shouldEnforceKillAfter(job) && job.executionStart)
            ? Math.max(1, Math.ceil((this.opt.killAfter * 1000 - executionTimeMs) / 1000))
            : Infinity
          job.visibilityTimeout = Math.min(doubled, secondsUntilMax, secondsUntilKill)
          job.extendAtSecond = Math.round(jobRunTime + job.visibilityTimeout / 2) // this is what we use next time
          jobsToExtend.push({
            job,
            previousVisibilityTimeout,
            visibilityTimeout: job.visibilityTimeout,
            extendAtSecond: job.extendAtSecond
          })
          debug({ doubled, secondsUntilMax, secondsUntilKill, job })
        }
      }
    }

    if (this.opt.verbose) {
      console.error(chalk.blue('Stats: '), { stats: this.stats, jobStatuses })
      console.error(chalk.blue('Jobs: '))
      for (const [qname, jobs] of this.jobsByQueue.entries()) {
        if (jobs.size) {
          const stats = {}
          for (const job of jobs) stats[job.status] = (stats[job.status] || 0) + 1
          console.error(chalk.blue('  queue:'), qname)
          if (stats.running) console.error(chalk.green('    running: '), stats.running)
          if (stats.waiting) console.error(chalk.yellow('    waiting: '), stats.waiting)
          if (stats.deleting) console.error(chalk.blue('    complete: '), stats.deleting)
          if (stats.failed) console.error(chalk.red('    failed: '), stats.failed)
        }
      }
    }

    // Extend in batches for each queue
    for (const qrl in jobsToExtendByQrl) {
      const jobsToExtend = jobsToExtendByQrl[qrl]
      debug({ qrl, jobsToExtend })
      while (jobsToExtend.length) {
        // Build list of messages to go in this batch
        const entries = []
        const batchJobs = {}
        let messageId = 0
        while (messageId++ < 10 && jobsToExtend.length) {
          const attempt = jobsToExtend.shift()
          const { job } = attempt
          // Another queue's request may have awaited a newer lease update for this job.
          if (job.visibilityTimeout !== attempt.visibilityTimeout ||
            job.extendAtSecond !== attempt.extendAtSecond) continue
          batchJobs[job.message.MessageId] = attempt
          const entry = {
            Id: job.message.MessageId,
            ReceiptHandle: job.message.ReceiptHandle,
            VisibilityTimeout: attempt.visibilityTimeout
          }
          entries.push(entry)
        }
        debug({ entries })
        if (!entries.length) continue

        // Change batch
        const input = { QueueUrl: qrl, Entries: entries }
        debug({ ChangeMessageVisibilityBatch: input })
        let result
        try {
          result = await getSQSMaintenanceClient().send(new ChangeMessageVisibilityBatchCommand(input))
        } catch (err) {
          // A single batch failure must not abort the whole maintenance run and
          // strand the remaining queues (or the end-of-pass cleanup). Log and
          // move on; jobs we couldn't extend get re-evaluated next cycle.
          debug('ChangeMessageVisibilityBatch error', err)
          if (this.opt.verbose) {
            console.error(chalk.red('FAILED_TO_EXTEND_BATCH'), { qrl, error: err?.message })
          } else if (!this.opt.disableLog) {
            console.log(JSON.stringify({
              event: 'EXTEND_VISIBILITY_ERROR',
              timestamp: start,
              qrl,
              count: entries.length,
              errorName: err?.name,
              errorMessage: err?.message
            }))
          }
          continue
        }
        debug('ChangeMessageVisibilityBatch returned', result)
        this.stats.sqsCalls++
        if (result.Failed?.length) {
          console.error('FAILED_MESSAGES', result.Failed)
          for (const failed of result.Failed) {
            const attempt = batchJobs[failed.Id]
            const job = attempt?.job
            console.error('FAILED_TO_EXTEND_JOB', { failedEntry: failed, job })
            // A late failure must not overwrite a newer visibility update for this delivery.
            if (!job || job.visibilityTimeout !== attempt.visibilityTimeout ||
              job.extendAtSecond !== attempt.extendAtSecond) continue

            // Keep execution custody, but do not treat a rejected lease as granted.
            job.visibilityTimeout = attempt.previousVisibilityTimeout
            if (failed.Code === 'ReceiptHandleIsInvalid' || failed.Code === 'MessageNotInflight') {
              job.extendAtSecond = Infinity
            } else {
              // FIFO completions can trigger early passes; bound retries independently of those passes.
              job.extendAtSecond = Math.ceil((Date.now() - job.start) / 1000) + 10
            }
          }
        }
        if (result.Successful?.length) {
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
        // Another pass's cleanup may untrack these jobs while this batch is in flight.
        const batchJobs = {}
        let messageId = 0
        while (messageId++ < 10 && jobsToDelete.length) {
          const job = jobsToDelete.shift()
          batchJobs[job.message.MessageId] = job
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
        let result
        try {
          result = await getSQSMaintenanceClient().send(new DeleteMessageBatchCommand(input))
        } catch (err) {
          // Don't let one failed delete batch abort the run and leave jobs
          // stranded in the 'deleting' state; they are cleaned up at the end of
          // the pass and any redelivery is guarded by dedup.
          debug('DeleteMessageBatch error', err)
          if (this.opt.verbose) {
            console.error(chalk.red('FAILED_TO_DELETE_BATCH'), { qrl, error: err?.message })
          } else if (!this.opt.disableLog) {
            console.log(JSON.stringify({
              event: 'DELETE_MESSAGES_ERROR',
              timestamp: start,
              qrl,
              count: entries.length,
              errorName: err?.name,
              errorMessage: err?.message
            }))
          }
          continue
        }
        this.stats.sqsCalls++
        if (result.Failed?.length) {
          console.error('FAILED_MESSAGES', result.Failed)
          for (const failed of result.Failed) {
            console.error('FAILED_TO_DELETE_JOB', { failedEntry: failed, job: batchJobs[failed.Id] })
            // ensure that we clean this one up so it doesn't generate api calls
            if (batchJobs[failed.Id]) batchJobs[failed.Id].status = 'failed'
          }
        }
        if (result.Successful?.length) {
          const count = result.Successful.length || 0
          this.stats.jobsDeleted += count
          if (this.opt.verbose) {
            console.error(chalk.blue('Deleted'), count, chalk.blue('jobs'))
          } else if (!this.opt.disableLog) {
            console.log(JSON.stringify({ event: 'DELETE_MESSAGES', timestamp: start, count, qrl }))
          }

          // Mark batch as processed for dedup
          await Promise.all(
            result.Successful.filter(e => batchJobs[e.Id]).map(
              e => dedupSuccessfullyProcessed(batchJobs[e.Id].message, this.opt)
            )
          )
        }
        debug('DeleteMessageBatch returned', result)

        // TODO Sentry
      }
    }

    // Get rid of deleted and failed jobs
    this.jobs = this.jobs.filter(job => {
      if (job.status === 'deleting' || job.status === 'failed') {
        debug('removed', job.message.MessageId)
        // Accounting
        delete this.jobsByMessageId[job.message.MessageId]
        this.jobsByQueue.get(job.qname).delete(job)
        return false
      } else {
        return true
      }
    })
  }

  addJob (message, callback, qname, qrl) {
    // Create job entry and track it
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

    // Accounting
    this.jobs.push(job)
    this.jobsByMessageId[job.message.MessageId] = job

    // Track all jobs for each queue
    if (!this.jobsByQueue.has(job.qname)) this.jobsByQueue.set(job.qname, new Set())
    this.jobsByQueue.get(job.qname).add(job)

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
      job.executionStart = new Date()
      this.stats.runningJobs++
      this.stats.waitingJobs--
      this.scheduleKillAfter(job)
      await this.setRunningVisibilityTimeout(job)
      const queue = job.qname.slice(this.opt.prefix.length)
      const attributes = {
        queueName: job.qname,
        messageId: job.message.MessageId || '',
        receiveCount: job.message.Attributes?.ApproximateReceiveCount || '1',
        sentTimestamp: job.message.Attributes?.SentTimestamp || '',
        firstReceiveTimestamp: job.message.Attributes?.ApproximateFirstReceiveTimestamp || '',
        messageGroupId: job.message.Attributes?.MessageGroupId || '',
        /** Register a child PID and whether it leads a dedicated process group. */
        registerPid: (pid, { processGroup = false } = {}) => {
          if (job.executionMode === 'inline') {
            debug('registerPid ignored after registerInlineExecution', { messageId: job.message?.MessageId })
            return
          }
          if (typeof pid !== 'number' || !Number.isInteger(pid) || pid <= 1 || pid === process.pid) {
            debug('registerPid: rejected invalid PID', pid)
            return
          }
          job.executionMode = 'child_process'
          job.pid = pid
          job.processGroup = !!processGroup
          if (job.killDue && !job.killed) this.killJob(job, new Date())
        },
        /** Call before inline work starts to opt out of kill-after visibility expiry. */
        registerInlineExecution: async () => {
          await this.registerInlineExecution(job)
        }
      }
      const result = await job.callback(queue, job.payload, attributes)
      if (job.killed) throw new Error('Job callback resolved after process termination was requested')
      debug('executeJob callback finished', { payload: job.payload, result })
      if (this.opt.verbose) {
        console.error(chalk.green('SUCCESS'), job.payload)
      }
      job.status = 'complete'
      // SQS holds the rest of a FIFO group until this message is deleted, so delete it now
      // instead of on the next 10s pass.
      if (!this.shutdownRequested && job.qrl.endsWith('.fifo') &&
        this.maintainPromises.size < maxPromptMaintenancePasses) {
        this.scheduleMaintenance(0)
      }

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
    } finally {
      this.clearJobTimers(job)
      this.stats.activeJobs--
      this.stats.runningJobs--
    }
  }

  async executeJobs (messages, callback, qname, qrl) {
    if (this.shutdownRequested) throw new Error('jobExecutor is shutting down so cannot execute new jobs')

    const executionPromise = this.executeJobsInternal(messages, callback, qname, qrl)
    this.executionPromises.add(executionPromise)
    try {
      await executionPromise
    } finally {
      this.executionPromises.delete(executionPromise)
    }
  }

  async executeJobsInternal (messages, callback, qname, qrl) {
    // Register the whole batch before callbacks start. If registration fails,
    // remove only this attempt's entries and leave its SQS messages for retry.
    const previousJobCount = this.jobs.length
    let jobs
    try {
      jobs = messages.map(message => this.addJob(message, callback, qname, qrl))
    } catch (error) {
      for (const job of this.jobs.splice(previousJobCount)) {
        delete this.jobsByMessageId[job.message.MessageId]
        this.jobsByQueue.get(job.qname).delete(job)
        this.stats.activeJobs--
        this.stats.waitingJobs--
      }
      throw error
    }
    if (!qrl.endsWith('.fifo')) {
      await Promise.all(jobs.map(job => this.runJob(job)))
      return
    }

    // Run independent groups together without letting a slow group hold up
    // another group in the same receive batch. Each group's order stays serial.
    const groups = new Map()
    for (const job of jobs) {
      const groupId = job.message?.Attributes?.MessageGroupId
      if (!groups.has(groupId)) groups.set(groupId, [])
      groups.get(groupId).push(job)
    }
    await Promise.all([...groups.values()].map(async group => {
      for (const job of group) await this.runJob(job)
    }))
  }
}
