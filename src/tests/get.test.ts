import ava, { TestFn } from 'ava'
import { createClient } from 'redis'
import transporter from '../index.js'

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
  try {
    const redisClient = createClient()
    await redisClient.connect()
    await redisClient.hSet('store:article:art1', redisData1)
    await redisClient.hSet('store:article:art2', redisData2)
    await redisClient.hSet('store:article:art3', redisData3)
    t.context = { redisClient }
  } catch (error) {
    console.error('Something went wrong:', error)
  }
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
