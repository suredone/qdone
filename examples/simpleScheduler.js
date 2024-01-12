#!/usr/bin/env -S node --experimental-json-modules

//
// This example implements a basic scheduler on top of qdone. The
// processMessages callback looks at the queue name throws a DoNotProcess
// error if there are already two messages processing on this queue.
//

import {
  enqueue,
  processMessages,
  requestShutdown
} from '../index.js' // from 'qdone' for standalone example

const randomEnqueue = setInterval(function () {
  const queue = ['rtest1', 'rtest2', 'rtest3'][Math.round(Math.random() * 2)]
  const payload = { foo: Math.round(Math.random() * 10) }
  console.log({ enqueue: { queue, payload } })
  enqueue(queue, JSON.stringify(payload))
}, 200)

process.on('SIGINT', () => { clearInterval(randomEnqueue); console.log('SIGINT'); requestShutdown() })
process.on('SIGTERM', () => { clearInterval(randomEnqueue); console.log('SIGTERM'); requestShutdown() })

// This returns a promise that resolves in the given number of milliseconds
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms))

// This keeps track of the number of active jobs per queue
const activeCount = {}

async function callback (queue, payload) {
  const numActive = activeCount[queue] = (activeCount[queue] || 0) + 1
  console.log({ numActive, activeCount })
  try {
    // Limit to 2 active tasks per queue
    if (numActive > 2) {
      console.log({ refusing: { queue, payload } })
    }

    console.log({ processing: { queue, payload } })
    await delay(Math.random() * 1000 * 2) // Processing takes up to 2 seconds
  } finally {
    activeCount[queue] = (activeCount[queue] || 0) - 1
  }
}

await processMessages(['rtest1', 'rtest2', 'rtest3'], callback, { verbose: true, disableLog: true })
