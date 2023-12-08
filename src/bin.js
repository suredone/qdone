#!/usr/bin/env -S node --no-warnings
import { run } from './cli.js'
await run(process.argv.slice(2))