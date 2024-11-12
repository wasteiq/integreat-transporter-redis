import test from 'ava'
import sinon from 'sinon'
import { createClient } from 'redis'
import type { Connection } from './types.js'

import connect from './connect.js'

// Setup

const client = {
  on: () => client,
  connect: async () => undefined,
} as unknown as ReturnType<typeof createClient>

// Tests

test('should return connection object with created redis client', async (t) => {
  const connectStub = sinon.stub().resolves()
  const clientWithConnect = {
    ...client,
    connect: connectStub,
  } as unknown as ReturnType<typeof createClient>
  const createClient = sinon.stub().returns(clientWithConnect)
  const options = { redis: { uri: 'redis://localhost:6379' } }

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.is(ret?.redisClient, clientWithConnect)
  t.is(ret?.expire, null)
  t.is(createClient.callCount, 1)
  t.deepEqual(createClient.args[0][0], { url: 'redis://localhost:6379' })
  t.is(connectStub.callCount, 1)
})

test('should set error handler before connecting', async (t) => {
  const connectStub = sinon.stub().resolves()
  const onStub = sinon.stub()
  const clientWithConnect = {
    ...client,
    connect: connectStub,
    on: onStub,
  } as unknown as ReturnType<typeof createClient>
  const createClient = sinon.stub().returns(clientWithConnect)
  const options = { redis: { uri: 'redis://localhost:6379' } }

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.true(onStub.calledBefore(connectStub))
})

test('should provide the expected options to Redis', async (t) => {
  const connectStub = sinon.stub().resolves()
  const clientWithConnect = {
    ...client,
    connect: connectStub,
  } as unknown as ReturnType<typeof createClient>
  const createClient = sinon.stub().returns(clientWithConnect)
  const options = {
    redis: {
      host: 'localhost',
      port: 6379,
      database: 1,
      tls: true,
      auth: {
        key: 'johnf',
        secret: 's3cr3t',
      },
    },
  }
  const expectedRedisOptions = {
    socket: {
      host: 'localhost',
      port: 6379,
      tls: true,
    },
    database: 1,
    username: 'johnf',
    password: 's3cr3t',
  }

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.is(ret?.redisClient, clientWithConnect)
  t.is(ret?.expire, null)
  t.is(createClient.callCount, 1)
  t.deepEqual(createClient.args[0][0], expectedRedisOptions)
  t.is(connectStub.callCount, 1)
})

test('should return connection object with credentials from auth object', async (t) => {
  const connectStub = sinon.stub().resolves()
  const clientWithConnect = {
    ...client,
    connect: connectStub,
  } as unknown as ReturnType<typeof createClient>
  const createClient = sinon.stub().returns(clientWithConnect)
  const options = {
    redis: {
      host: 'localhost',
      port: 6379,
      database: 1,
      tls: true,
    },
  }
  const auth = { status: 'granted', key: 'johnf', secret: 's3cr3t' }
  const expectedRedisOptions = {
    socket: {
      host: 'localhost',
      port: 6379,
      tls: true,
    },
    database: 1,
    username: 'johnf',
    password: 's3cr3t',
  }

  const ret = await connect(createClient)(options, auth, null)

  t.is(ret?.status, 'ok')
  t.is(ret?.redisClient, clientWithConnect)
  t.is(ret?.expire, null)
  t.is(createClient.callCount, 1)
  t.deepEqual(createClient.args[0][0], expectedRedisOptions)
  t.is(connectStub.callCount, 1)
})

test('should set expire when an connectionTimeout interval is given', async (t) => {
  const createClient = sinon.stub().returns(client)
  const options = {
    redis: { uri: 'redis://localhost:6379' },
    connectionTimeout: 60000,
  }
  const expectedExpire = Date.now() + 60000

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.is(typeof ret?.expire, 'number')
  t.true((ret?.expire as number) >= expectedExpire)
  t.true((ret?.expire as number) < expectedExpire + 100) // Give it a few ms in case of delay
})

test('should return existing client when given', async (t) => {
  const createClient = sinon.stub().returns(client)
  const options = { redis: { uri: 'redis://localhost:6379' } }
  const connection = { status: 'ok', redisClient: client }

  const ret = await connect(createClient)(options, null, connection)

  t.is(ret, connection)
  t.is(createClient.callCount, 0)
})

test('should reconnect when a connection is missing client', async (t) => {
  const createClient = sinon.stub().returns(client)
  const options = { redis: { uri: 'redis://localhost:6379' } }
  const connection = { status: 'ok', redisClient: null }

  const ret = await connect(createClient)(options, null, connection)

  t.is(ret?.status, 'ok')
  t.is(ret?.redisClient, client)
  t.is(createClient.callCount, 1)
})

test('should reconnect when connection is expired', async (t) => {
  const clientWithQuit = {
    ...client,
    quit: sinon.stub().resolves(),
    isOpen: true,
  }
  const createClient = sinon.stub().returns(client)
  const options = { redis: { uri: 'redis://localhost:6379' } }
  const connection = {
    status: 'ok',
    redisClient: clientWithQuit,
    expire: Date.now() - 1000,
  }

  const ret = await connect(createClient)(
    options,
    null,
    connection as unknown as Connection,
  )

  t.is(ret?.status, 'ok')
  t.is(ret?.redisClient, client)
  t.is(createClient.callCount, 1)
  t.is(clientWithQuit.quit.callCount, 1)
})

test('should return error when no redis options', async (t) => {
  const createClient = sinon.stub().returns({})
  const options = {}
  const expectedConnection = {
    status: 'error',
    error: 'No redis options',
    redisClient: null,
  }

  const ret = await connect(createClient)(options, null, null)

  t.deepEqual(ret, expectedConnection)
  t.is(createClient.callCount, 0)
})

test('should set incoming options', async (t) => {
  const createClient = sinon.stub().returns(client)
  const options = {
    redis: { uri: 'redis://localhost:6379' },
    incoming: { keyPattern: 'store:user:*' },
  }
  const expectedIncoming = { keyPattern: 'store:user:*' }

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.deepEqual(ret?.incoming, expectedIncoming)
})

test('should include prefix from service options in incoming keyPattern', async (t) => {
  const createClient = sinon.stub().returns(client)
  const options = {
    redis: { uri: 'redis://localhost:6379' },
    incoming: { keyPattern: 'user:*' },
    prefix: 'store',
  }
  const expectedIncoming = { keyPattern: 'store:user:*' }

  const ret = await connect(createClient)(options, null, null)

  t.is(ret?.status, 'ok')
  t.deepEqual(ret?.incoming, expectedIncoming)
})
