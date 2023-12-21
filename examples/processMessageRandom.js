#!/usr/bin/env -S node --experimental-json-modules
// 
// This example processes random messages once a second until you Ctl-C
//

import { enqueue, processMessages, requestShutdown } from '../index.mjs' // from 'qdone' for standalone example

const randomEnqueue = setInterval(function () {
  enqueue(['rtest1', 'rtest2', 'rtest3'][Math.round(Math.random()*2)], JSON.stringify({foo: Math.round(Math.random() * 10)}))
}, 1000)

process.on('SIGINT', () => { clearInterval(randomEnqueue); console.log('SIGINT'); requestShutdown() })
process.on('SIGTERM', () => { clearInterval(randomEnqueue); console.log('SIGTERM'); requestShutdown() })

async function callback (queue, payload) {
  console.log({ queue, payload })
}

await processMessages(['rtest1', 'rtest2', 'rtest3'], callback, { verbose: true, disableLog: true })
