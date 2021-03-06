/* eslint-env mocha */

const chai = require('chai')
const cache = require('../src/cache')
const expect = chai.expect

beforeEach(function () { cache.resetClient() })
after(function () { cache.resetClient() })

describe('cache', function () {
  const options = {
    'cache-uri': 'redis://localhost',
    'cache-ttl-seconds': 10,
    'cache-prefix': 'qdone:'
  }

  describe('getClient', function () {
    it('should throw an error for invalid cache uri', function () {
      const badOptions = Object.assign({}, options, { 'cache-uri': 'bob://foo' })
      expect(() => cache.getClient(badOptions)).to.throw(/currently supported/)
    })
    it('should throw an error when --cache-uri is missing', function () {
      const badOptions = Object.assign({}, options)
      delete badOptions['cache-uri']
      expect(() => cache.getClient(badOptions)).to.throw(/--cache-uri/)
    })
    it('should support redis:// URIs', function () {
      const client = cache.getClient(options)
      return expect(client).to.be.ok
    })
    it('should support redis-cache:// URIs', function () {
      const client = cache.getClient(Object.assign({}, options, { 'cache-uri': 'redis-cluster://' }))
      return expect(client).to.be.ok
    })
    it('should return an identical client on subsequent calls', function () {
      const client1 = cache.getClient(options)
      const client2 = cache.getClient(options)
      return expect(client1).to.be.equal(client2)
    })
  })

  describe('setCache', function () {
    it('should successfully set a value', function () {
      return cache.setCache('test', { one: 1 }, options)
        .then(result => expect(result).to.equal('OK'))
    })
  })

  describe('getCache', function () {
    it('should return the same value', function () {
      return cache.getCache('test', options).then(
        result => expect(JSON.stringify(result)).to.equal(JSON.stringify({ one: 1 }))
      )
    })
    it('should return undefined for nonexistent keys', function () {
      return cache.getCache('supercalafragalisticexpealadocious', options).then(
        result => expect(result).to.be.undefined
      )
    })
  })
})
