/* eslint-env mocha */

const expect = require('chai').expect
// const exec = require('child_process').exec
const sinon = require('sinon')
const cli = require('../src/cli')
const path = require('path')
var sandbox

var originalArgv = process.argv

beforeEach(function () {
  process.argv = [
    originalArgv[0], // node executable
    path.join(process.cwd(), 'qdone')
  ]
  sandbox = sinon.sandbox.create()
  sandbox.stub(process, 'exit')
  sandbox.stub(process.stdout, 'write')
  sandbox.stub(process.stderr, 'write')
})

afterEach(function () {
  sandbox.restore()
  // console.error('      process.argv', process.argv)
  process.argv = originalArgv
})

describe('cli', function () {
  describe('qdone', function () {
    it('should print help and exit 1', function () {
      try {
        cli.run(process.argv)
        expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('Usage: ')
        expect(process.exit.args[0][0]).to.equal(1)
      } finally {
        sandbox.restore()
      }
    })
  })

  describe('qdone --help', function () {
    it('should print help and exit 0', function () {
      try {
        process.argv.push('--help')
        cli.run(process.argv)
        expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('Usage: ')
        expect(process.exit.args[0][0]).to.equal(0)
      } finally {
        sandbox.restore()
      }
    })
  })
})

describe('cli enqueue', function () {
  describe('qdone enqueue', function () {
    it('should exit 1', function () {
      try {
        process.argv.push('enqueue')
        cli.run(process.argv)
        // expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('missing required argument')
        expect(process.exit.args[0][0]).to.equal(1)
      } finally {
        sandbox.restore()
      }
    })
  })

  describe('qdone enqueue --help', function () {
    it('should return help and exit 0', function () {
      try {
        process.argv.push('enqueue')
        process.argv.push('--help')
        cli.run(process.argv)
        expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('Usage: ')
        expect(process.exit.args[0][0]).to.equal(0)
      } finally {
        sandbox.restore()
      }
    })
  })
})

/*
describe('cli worker', function () {
  describe('qdone worker', function () {
    it('should print error and exit 1', function () {
      try {
        process.argv.push('worker')
        cli.run(process.argv)
        expect(process.stderr.write.args.reduce((a, b) => a + b, '')).to.contain('missing required argument')
        expect(process.exit.args[0][0]).to.equal(1)
      } finally {
        sandbox.restore()
      }
    })
  })

  describe('qdone worker --help', function () {
    it('should return help and exit 0', function () {
      try {
        process.argv.push('worker')
        process.argv.push('--help')
        cli.run(process.argv)
        expect(process.stdout.write.args.reduce((a, b) => a + b, '')).to.contain('Usage: ')
        expect(process.exit.args[0][0]).to.equal(0)
      } finally {
        sandbox.restore()
      }
    })
  })
})
*/
