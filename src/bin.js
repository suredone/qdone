#!/usr/bin/env -S node --no-warnings --experimental-json-modules
import { run } from './cli.js'
await run(process.argv.slice(2))
