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
const maxBackgroundVisibilityRequests = 4
const visibilityRetryDelayMs = 10000

/**
 * @typedef {object} VisibilityRequest
 * @property {string} receiptHandle
 * @property {number} visibilityTimeout
 * @property {number} sentAt
 * @property {Promise<void>} settled
 * @property {() => void} release
 */

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
      jobsKilled: 0,
      jobsSkipped: 0
    }
    this.executionPromises = new Set()
    this.killEscalationPromises = new Set()
    this.maintainPromises = new Set()
    this.backgroundVisibilityWorkers = 0
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

  /** @param {object} job @param {string} receiptHandle @param {string} code @returns {boolean} */
  abandonUnenteredReceipt (job, receiptHandle, code) {
    if (code !== 'ReceiptHandleIsInvalid' && code !== 'MessageNotInflight') return false
    if (this.jobsByMessageId[job.message.MessageId] !== job ||
      job.message.ReceiptHandle !== receiptHandle || job.callbackEntered ||
      (job.status !== 'waiting' && job.status !== 'running')) return false

    this.stats.activeJobs--
    if (job.status === 'waiting') this.stats.waitingJobs--
    else this.stats.runningJobs--
    job.status = 'abandoned'
    this.clearJobTimers(job)
    this.jobs = this.jobs.filter(tracked => tracked !== job)
    this.jobsByQueue.get(job.qname).delete(job)
    if (this.jobsByMessageId[job.message.MessageId] === job) {
      delete this.jobsByMessageId[job.message.MessageId]
    }
    this.stats.jobsSkipped++
    if (this.opt.verbose) {
      console.error(chalk.yellow('RECEIPT_LOST_BEFORE_CALLBACK'), job.qname, job.message.MessageId, code)
    } else if (!this.opt.disableLog) {
      console.log(JSON.stringify({
        event: 'RECEIPT_LOST_BEFORE_CALLBACK',
        timestamp: new Date(),
        queue: job.qname,
        messageId: job.message.MessageId,
        code
      }))
    }
    return true
  }

  /** @param {object} job @param {string} receiptHandle @returns {boolean} */
  isCurrentVisibilityReceipt (job, receiptHandle) {
    return this.jobsByMessageId[job.message.MessageId] === job &&
      job.message.ReceiptHandle === receiptHandle && !job.visibilityLost &&
      (job.status === 'waiting' || job.status === 'running')
  }

  /** @param {object} job @param {number} now @returns {boolean} */
  isVisibilityDue (job, now) {
    if (job.visibilityRequest || !this.isCurrentVisibilityReceipt(job, job.message.ReceiptHandle)) return false
    if (job.visibilityRetryAt !== undefined) return now >= job.visibilityRetryAt
    return Math.round((now - job.start) / 1000) >= job.extendAtSecond
  }

  /** @param {object} job @param {number} now @returns {number} */
  visibilityKillBudget (job, now) {
    if (!this.shouldEnforceKillAfter(job) || !job.executionStart) return Infinity
    const elapsed = this.getExecutionTimeMs(job, now)
    return Math.max(1, Math.ceil((this.opt.killAfter * 1000 - elapsed) / 1000))
  }

  /** @param {object} job @returns {boolean} */
  needsInlineVisibilityRestore (job) {
    return job.executionMode === 'inline' &&
      (job.visibilityTimeout < defaultVisibilityTimeout || !!job.possiblyShortVisibility)
  }

  /** @param {object} job @param {string} receiptHandle @param {number} visibilityTimeout @param {number} sentAt @returns {VisibilityRequest} */
  beginVisibilityRequest (job, receiptHandle, visibilityTimeout, sentAt) {
    let release
    const settled = new Promise(resolve => { release = resolve })
    const request = { receiptHandle, visibilityTimeout, sentAt, settled, release }
    job.visibilityRequest = request
    return request
  }

  /** @param {object} job @param {VisibilityRequest} request @returns {void} */
  releaseVisibilityRequest (job, request) {
    if (job.visibilityRequest === request) delete job.visibilityRequest
    request.release()
  }

  /** @param {object} job @param {VisibilityRequest} request @param {boolean} confirmed @param {string | undefined} code @returns {boolean} */
  applyVisibilityReply (job, request, confirmed, code) {
    if (job.visibilityRequest !== request || !this.isCurrentVisibilityReceipt(job, request.receiptHandle)) return false
    if (confirmed) {
      job.visibilityTimeout = request.visibilityTimeout
      job.confirmedVisibilitySentAt = request.sentAt
      job.extendAtSecond = Math.round((request.sentAt - job.start) / 1000 + request.visibilityTimeout / 2)
      delete job.visibilityRetryAt
      if (request.visibilityTimeout >= defaultVisibilityTimeout) delete job.possiblyShortVisibility
      this.stats.timeoutsExtended++
      return true
    }

    if (code === 'ReceiptHandleIsInvalid' || code === 'MessageNotInflight') {
      job.visibilityLost = true
      delete job.possiblyShortVisibility
      this.abandonUnenteredReceipt(job, request.receiptHandle, code)
    } else {
      job.visibilityRetryAt = Date.now() + visibilityRetryDelayMs
      if (request.visibilityTimeout < defaultVisibilityTimeout) job.possiblyShortVisibility = true
    }
    return false
  }

  /** @param {object} job @param {number | ((now: number) => (number | undefined))} timeout @returns {Promise<void>} */
  async setJobVisibilityTimeout (job, timeout) {
    const receiptHandle = job.message.ReceiptHandle
    while (job.visibilityRequest) await job.visibilityRequest.settled
    if (!this.isCurrentVisibilityReceipt(job, receiptHandle)) return

    // Compute a clamp or restore only after the preceding operation settles.
    const sentAt = Date.now()
    const visibilityTimeout = typeof timeout === 'function' ? timeout(sentAt) : timeout
    if (visibilityTimeout === undefined) return
    const request = this.beginVisibilityRequest(job, receiptHandle, visibilityTimeout, sentAt)
    const input = { QueueUrl: job.qrl, ReceiptHandle: receiptHandle, VisibilityTimeout: visibilityTimeout }
    debug({ ChangeMessageVisibility: input })

    try {
      let result
      try {
        result = await getSQSMaintenanceClient().send(new ChangeMessageVisibilityCommand(input))
      } catch (err) {
        this.applyVisibilityReply(job, request, false, err?.name)
        debug('ChangeMessageVisibility error', err)
        if (this.opt.verbose) {
          console.error(chalk.red('FAILED_TO_SET_VISIBILITY_TIMEOUT'), { err, input })
        }
        return
      }
      debug('ChangeMessageVisibility returned', result)
      this.stats.sqsCalls++
      this.applyVisibilityReply(job, request, true, undefined)
    } finally {
      this.releaseVisibilityRequest(job, request)
    }
  }

  /** @param {object} job @returns {Promise<void>} */
  setRunningVisibilityTimeout (job) {
    if (!this.shouldEnforceKillAfter(job)) return Promise.resolve()
    return this.setJobVisibilityTimeout(job, now => {
      const remaining = this.visibilityKillBudget(job, now)
      return remaining < job.visibilityTimeout ? remaining : undefined
    })
  }

  /** @param {object} job @returns {Promise<void>} */
  async registerInlineExecution (job) {
    if (job.executionMode === 'inline') return
    if (job.executionMode === 'child_process') {
      debug('registerInlineExecution ignored after registerPid', { messageId: job.message?.MessageId })
      return
    }

    job.executionMode = 'inline'
    job.killDue = false
    this.clearJobTimers(job)

    const pending = job.visibilityRequest
    if (!this.needsInlineVisibilityRestore(job) &&
      (!pending || pending.visibilityTimeout >= defaultVisibilityTimeout)) return

    await this.setJobVisibilityTimeout(job, () => {
      if (job.status === 'running' && this.needsInlineVisibilityRestore(job)) return defaultVisibilityTimeout
    })
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
      } else if (job.status === 'waiting' || job.status === 'running') {
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

        if (this.isVisibilityDue(job, +start)) {
          const jobsToExtend = jobsToExtendByQrl[job.qrl] || []
          jobsToExtend.push(job)
          jobsToExtendByQrl[job.qrl] = jobsToExtend
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

    const results = await Promise.allSettled([
      this.extendVisibilityBatches(jobsToExtendByQrl),
      this.deleteCompletedJobs(jobsToDeleteByQrl, start)
    ])
    // Keep the existing pass boundary for untracking pending deletes.
    if (results[1].status === 'fulfilled') this.cleanupFinishedJobs()
    const failures = results.filter(result => result.status === 'rejected').map(result => result.reason)
    if (failures.length === 1) throw failures[0]
    if (failures.length) throw new AggregateError(failures, 'QDone visibility and deletion maintenance failed')
  }

  /** @param {Record<string, object[]>} jobsToExtendByQrl @returns {Promise<void>} */
  async extendVisibilityBatches (jobsToExtendByQrl) {
    const queues = Object.entries(jobsToExtendByQrl).map(([qrl, jobs]) => ({ qrl, jobs, offset: 0 }))
    let nextQueue = 0
    const extend = async () => {
      while (queues.length) {
        const queue = queues[nextQueue]
        const jobs = queue.jobs.slice(queue.offset, queue.offset + 10)
        queue.offset += jobs.length
        if (queue.offset === queue.jobs.length) queues.splice(nextQueue, 1)
        else nextQueue++
        if (nextQueue >= queues.length) nextQueue = 0
        await this.extendVisibilityBatch(queue.qrl, jobs)
      }
    }
    const workers = []
    // Interleave queues within a pass and share the limit with other passes.
    while (queues.length && this.backgroundVisibilityWorkers < maxBackgroundVisibilityRequests) {
      this.backgroundVisibilityWorkers++
      workers.push(extend().finally(() => { this.backgroundVisibilityWorkers-- }))
    }
    const results = await Promise.allSettled(workers)
    const failures = results.filter(result => result.status === 'rejected').map(result => result.reason)
    if (failures.length === 1) throw failures[0]
    if (failures.length) throw new AggregateError(failures, 'QDone visibility batches failed')
  }

  /** @param {string} qrl @param {object[]} jobs @returns {Promise<void>} */
  async extendVisibilityBatch (qrl, jobs) {
    const sentAt = Date.now()
    const entries = []
    const requests = new Map()
    for (const job of jobs) {
      if (!this.isVisibilityDue(job, sentAt)) continue
      const jobRunTime = Math.round((sentAt - job.start) / 1000)
      const desired = this.needsInlineVisibilityRestore(job) ? defaultVisibilityTimeout : job.visibilityTimeout * 2
      const visibilityTimeout = Math.min(desired, Math.max(1, maxJobSeconds - jobRunTime), this.visibilityKillBudget(job, sentAt))
      const receiptHandle = job.message.ReceiptHandle
      const request = this.beginVisibilityRequest(job, receiptHandle, visibilityTimeout, sentAt)
      requests.set(job.message.MessageId, { job, request })
      entries.push({ Id: job.message.MessageId, ReceiptHandle: receiptHandle, VisibilityTimeout: visibilityTimeout })
    }
    if (!entries.length) return

    const input = { QueueUrl: qrl, Entries: entries }
    debug({ ChangeMessageVisibilityBatch: input })
    try {
      let result
      try {
        result = await getSQSMaintenanceClient().send(new ChangeMessageVisibilityBatchCommand(input))
      } catch (err) {
        for (const { job, request } of requests.values()) this.applyVisibilityReply(job, request, false, undefined)
        debug('ChangeMessageVisibilityBatch error', err)
        if (this.opt.verbose) {
          console.error(chalk.red('FAILED_TO_EXTEND_BATCH'), { qrl, error: err?.message })
        } else if (!this.opt.disableLog) {
          console.log(JSON.stringify({
            event: 'EXTEND_VISIBILITY_ERROR',
            timestamp: new Date(sentAt),
            qrl,
            count: entries.length,
            errorName: err?.name,
            errorMessage: err?.message
          }))
        }
        return
      }

      debug('ChangeMessageVisibilityBatch returned', result)
      this.stats.sqsCalls++
      const successful = result?.Successful === undefined ? [] : result.Successful
      const failed = result?.Failed === undefined ? [] : result.Failed
      const replies = new Map()
      if (Array.isArray(successful) && Array.isArray(failed)) {
        for (const [group, confirmed] of [[successful, true], [failed, false]]) {
          for (const entry of group) {
            if (!requests.has(entry?.Id)) continue
            // A duplicate or conflicting ID is ambiguous even when its code is terminal.
            replies.set(entry.Id, replies.has(entry.Id) ? undefined : { confirmed, code: entry.Code })
          }
        }
      }
      let count = 0
      for (const [id, { job, request }] of requests) {
        const reply = replies.get(id)
        if (reply && !reply.confirmed) console.error('FAILED_TO_EXTEND_JOB', { messageId: id, qrl, code: reply.code })
        if (this.applyVisibilityReply(job, request, reply?.confirmed === true, reply?.code)) count++
      }
      if (count && this.opt.verbose) {
        console.error(chalk.blue('Extended'), count, chalk.blue('jobs'))
      } else if (count && !this.opt.disableLog) {
        console.log(JSON.stringify({ event: 'EXTEND_VISIBILITY_TIMEOUTS', timestamp: new Date(sentAt), count, qrl }))
      }
    } finally {
      for (const { job, request } of requests.values()) this.releaseVisibilityRequest(job, request)
    }
  }

  /** @param {Record<string, object[]>} jobsToDeleteByQrl @param {Date} start @returns {Promise<void>} */
  async deleteCompletedJobs (jobsToDeleteByQrl, start) {
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
  }

  /** @returns {void} */
  cleanupFinishedJobs () {
    this.jobs = this.jobs.filter(job => {
      if (job.status === 'deleting' || job.status === 'failed') {
        debug('removed', job.message.MessageId)
        // Accounting
        if (this.jobsByMessageId[job.message.MessageId] === job) {
          delete this.jobsByMessageId[job.message.MessageId]
        }
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
      confirmedVisibilitySentAt: null,
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
    if (job.status === 'abandoned') return
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
      if (job.status === 'abandoned') return
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
      job.callbackEntered = true
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
      if (job.status !== 'abandoned') {
        this.stats.activeJobs--
        this.stats.runningJobs--
      }
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
        if (this.jobsByMessageId[job.message.MessageId] === job) {
          delete this.jobsByMessageId[job.message.MessageId]
        }
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
