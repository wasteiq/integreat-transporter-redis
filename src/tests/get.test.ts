import ava, { TestFn } from 'ava'
import { createClient } from 'redis'
import transporter from '../index.js'
import { setTimeout } from 'timers/promises'

interface RedisContext {
  redisClient: ReturnType<typeof createClient>
}

// Setup

const test = ava as TestFn<RedisContext>

const redisData1 = [
  'title',
  'Article 1',
  'description',
  'The first article',
  'publishedAt',
  '##null##',
  'author',
  JSON.stringify({ id: 'johnf', name: 'John F.' }),
]
const redisData2 = [
  'title',
  'Article 2',
  'description',
  'The second article',
  'publishedAt',
  '2023-11-18T09:14:44Z',
  'author',
  JSON.stringify({ id: 'lucyk', name: 'Lucy K.' }),
]
const redisData3 = [
  'title',
  'Article 3',
  'description',
  'The third article',
  'publishedAt',
  '##null##',
  'author',
  JSON.stringify({ id: 'johnf', name: 'John F.' }),
]

test.before(async (t) => {
  const redisClient = createClient()
  await redisClient.connect()
  await redisClient.hSet('store:article:art1', redisData1)
  await redisClient.hSet('store:article:art2', redisData2)
  await redisClient.hSet('store:article:art3', redisData3)
  t.context = { redisClient }
})

test.after.always(async (t) => {
  const { redisClient } = t.context
  if (redisClient) {
    await redisClient.del('store:article:art1')
    await redisClient.del('store:article:art2')
    await redisClient.del('store:article:art3')
    await redisClient.quit()
  }
})

const emit = () => undefined

// Tests

test('should get data from redis service', async (t) => {
  const options = {
    prefix: 'store',
    redis: {
      uri: 'redis://localhost:6379',
    },
  }
  const action = {
    type: 'GET',
    payload: {
      type: 'article',
      id: 'art1',
    },
    meta: {
      options,
    },
  }
  const expectedData = {
    id: 'art1',
    title: 'Article 1',
    description: 'The first article',
    publishedAt: null,
    author: { id: 'johnf', name: 'John F.' },
  }

  const client = await transporter.connect(options, null, null, emit)
  const ret = await transporter.send(action, client)
  await transporter.disconnect(client)

  t.is(ret.status, 'ok')
  t.deepEqual(ret.data, expectedData)
})

test('should be able to reconnect to Redis if the server has disconnected', async (t) => {
  const options = {
    prefix: 'store',
    redis: {
      uri: 'redis://localhost:6379',
    },
    connectionTimeout: 5,
  }
  const action1 = {
    type: 'GET',
    payload: {
      type: 'article',
      id: 'art1',
    },
    meta: {
      options,
    },
  }
  const action2 = {
    type: 'GET',
    payload: {
      type: 'article',
      id: 'art2',
    },
    meta: {
      options,
    },
  }
  const expectedData1 = {
    id: 'art1',
    title: 'Article 1',
    description: 'The first article',
    publishedAt: null,
    author: { id: 'johnf', name: 'John F.' },
  }
  const expectedData2 = {
    id: 'art2',
    title: 'Article 2',
    description: 'The second article',
    publishedAt: '2023-11-18T09:14:44Z',
    author: { id: 'lucyk', name: 'Lucy K.' },
  }
  const connection = await transporter.connect(options, null, null, emit)
  const ret1 = await transporter.send(action1, connection)

  const redisClient = connection?.redisClient as ReturnType<typeof createClient>
  await redisClient.quit()

  // Wait for the connection to expire which should trigger an attempt to
  // disconnect (and client.quit() under the hood) and then reconnect on
  // the next call to transporter.connect()
  await setTimeout(options.connectionTimeout + 1)

  // Call transporter.connect(), with the existing connection as a parameter,
  // to emulate the call to connection.connect() that happens in
  // the sendToTransporter function in integreat "core"
  // https://github.com/integreat-io/integreat/blob/main/src/service/utils/send.ts#L12
  const newConnection = await transporter.connect(
    options,
    null,
    connection,
    emit,
  )
  const ret2 = await transporter.send(action2, newConnection)

  t.is(ret1.status, 'ok')
  t.deepEqual(ret1.data, expectedData1)
  t.is(ret2.status, 'ok')
  t.deepEqual(ret2.data, expectedData2)
})

test('should get data with several ids', async (t) => {
  const options = {
    prefix: 'store',
    redis: {
      uri: 'redis://localhost:6379',
    },
  }
  const action = {
    type: 'GET',
    payload: {
      type: 'article',
      id: ['art1', 'art3'],
    },
    meta: {
      options,
    },
  }

  const client = await transporter.connect(options, null, null, emit)
  const ret = await transporter.send(action, client)
  await transporter.disconnect(client)

  t.is(ret.status, 'ok')
  t.true(Array.isArray(ret.data))
  const data = ret.data as Record<string, string>[]
  t.is(data.length, 2)
  t.is(data[0].id, 'art1')
  t.is(data[1].id, 'art3')
})

test('should get data with pattern', async (t) => {
  const options = {
    prefix: 'store',
    redis: {
      uri: 'redis://localhost:6379',
    },
  }
  const action = {
    type: 'GET',
    payload: {
      pattern: 'article',
    },
    meta: {
      options,
    },
  }
  const expectedItem0 = {
    id: 'article:art1', // We get the pattern as part of the id as we don't have a type
    title: 'Article 1',
    description: 'The first article',
    publishedAt: null,
    author: { id: 'johnf', name: 'John F.' },
  }

  const client = await transporter.connect(options, null, null, emit)
  const ret = await transporter.send(action, client)
  await transporter.disconnect(client)

  t.is(ret.status, 'ok')
  t.true(Array.isArray(ret.data))
  const data = ret.data as Record<string, string>[]
  t.is(data.length, 3)
  t.deepEqual(data[0], expectedItem0)
  t.is(data[1].id, 'article:art2')
  t.is(data[2].id, 'article:art3')
})

test('should get only ids with pattern', async (t) => {
  const options = {
    prefix: 'store',
    redis: {
      uri: 'redis://localhost:6379',
    },
  }
  const action = {
    type: 'GET',
    payload: {
      pattern: 'article',
      onlyIds: true,
    },
    meta: {
      options,
    },
  }

  const client = await transporter.connect(options, null, null, emit)
  const ret = await transporter.send(action, client)
  await transporter.disconnect(client)

  t.is(ret.status, 'ok')
  t.true(Array.isArray(ret.data))
  const data = ret.data as Record<string, string>[]
  t.is(data.length, 3)
  t.deepEqual(data[0], { id: 'article:art1' })
  t.deepEqual(data[1], { id: 'article:art2' })
  t.deepEqual(data[2], { id: 'article:art3' })
})
