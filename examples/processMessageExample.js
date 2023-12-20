#!/usr/bin/env -S node --experimental-json-modules
import { enqueue, processMessages, requestShutdown } from 'qdone'

const randomEnqueue = setInterval(function () {
  enqueue(['rtest1', 'rtest2', 'rtest3'][Math.round(Math.random()*2)], JSON.stringify({foo: Math.round(Math.random() * 10)}))
}, 1000)

process.on('SIGINT', () => { clearInterval(randomEnqueue); console.log('SIGINT'); requestShutdown() })
process.on('SIGTERM', () => { clearInterval(randomEnqueue); console.log('SIGTERM'); requestShutdown() })

//await enqueue('test1', JSON.stringify({one: 1}))
//await enqueue('test2', JSON.stringify({two: 2}))
//await enqueue('test3', JSON.stringify({three: 3}))

async function callback (queue, payload) {
  console.log({ queue, payload })
  //if (payload.three) requestShutdown()
}

await processMessages(['rtest1', 'rtest2', 'rtest3'], callback, { verbose: true, disableLog: true })
