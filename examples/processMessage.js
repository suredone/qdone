#!/usr/bin/env -S node --experimental-json-modules
// 
// This example enqueues three messages and processes them.
//
import { enqueue, processMessages, requestShutdown } from 'qdone'

await enqueue('test1', JSON.stringify({one: 1}))
await enqueue('test2', JSON.stringify({two: 2}))
await enqueue('test3', JSON.stringify({three: 3}))

async function callback (queue, payload) {
  console.log({ queue, payload })
  if (payload.three) requestShutdown()
}

await processMessages(['rtest1', 'rtest2', 'rtest3'], callback)
