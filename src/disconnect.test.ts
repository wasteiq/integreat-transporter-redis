import test from 'ava'
import sinon from 'sinon'
import type { Connection } from './types.js'

import disconnect from './disconnect.js'

// Tests

test('should call quit on redis client', async (t) => {
  const redisClient = {
    quit: sinon.stub().resolves(),
    isOpen: true,
  }
  const connection = {
    status: 'ok',
    redisClient: redisClient,
  } as unknown as Connection

  await disconnect(connection)

  t.is(redisClient.quit.callCount, 1)
  t.is(connection.redisClient, null)
})

test('should handle that quit throws even though client reports isOpen', async (t) => {
  const redisClient = {
    quit: sinon.stub().throws('Error'),
    isOpen: true,
  }
  const connection = {
    status: 'ok',
    redisClient: redisClient,
  } as unknown as Connection

  await disconnect(connection)

  t.is(redisClient.quit.callCount, 1)
  t.is(connection.redisClient, null)
})

test('should not call quit on redis client when the client is already closed', async (t) => {
  const redisClient = {
    quit: sinon.stub().resolves(),
    isOpen: false,
  }
  const connection = {
    status: 'ok',
    redisClient: redisClient,
  } as unknown as Connection

  await disconnect(connection)

  t.is(redisClient.quit.callCount, 0)
  t.is(connection.redisClient, null)
})

test('should do nothing when no connection', async (t) => {
  await t.notThrowsAsync(disconnect(null))
})

test('should do nothing when no client', async (t) => {
  const connection = { status: 'error', error: 'Fail', redisClient: null }

  await t.notThrowsAsync(disconnect(connection))
})
