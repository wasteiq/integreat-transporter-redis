import FakeTimers from '@sinonjs/fake-timers'
import test from 'ava'
import Docker from 'dockerode'
import { createClient } from 'redis'
import transporter from '../index.js'

const emit = () => undefined

test.serial(
  'should be able to reconnect to Redis if the server has disconnected',
  async (t) => {
    const clock = FakeTimers.install({ toFake: ['Date'], now: Date.now() })
    // Create and connect the RedisClient
    const redisClient = createClient()
    await redisClient.connect()

    const options = {
      prefix: 'store',
      redis: {
        uri: 'redis://localhost:6380',
      },
      connectionTimeout: 1,
    }

    const pingAction = {
      type: 'PING',
      payload: {},
      meta: {},
    }
    const connection = await transporter.connect(options, null, null, emit)
    const firstActionResult = await transporter.send(pingAction, connection)

    // Disconnect the Redis client from the network
    const docker = new Docker({ socketPath: '/var/run/docker.sock' })
    const networks: Docker.NetworkInspectInfo[] = await docker.listNetworks()
    const network = networks.find(
      (network) => network.Name === 'integreat-transporter-redis_reconnect',
    )
    t.truthy(network, 'Network not found')
    const networkId = network?.Id ?? 'N/A'

    await docker
      .getNetwork(networkId)
      .disconnect({ Container: 'reconnect-redis' })

    // Wait for the connection to expire which should trigger an attempt to
    // disconnect (and client.quit() or client.disconnect() under the hood)
    // and then reconnect on the next call to transporter.connect()
    clock.tick(options.connectionTimeout + 1)
    clock.uninstall()

    // Reconnect to the network after options.connectionTimeout + 2 ms
    setTimeout(async () => {
      await docker
        .getNetwork(networkId)
        .connect({ Container: 'reconnect-redis' })
    }, options.connectionTimeout + 2)

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
    const secondActionResult = await transporter.send(pingAction, newConnection)

    t.is(firstActionResult.status, 'ok')
    t.deepEqual(firstActionResult.data, 'PONG')
    t.is(secondActionResult.status, 'ok')
    t.deepEqual(secondActionResult.data, 'PONG')
  },
)
