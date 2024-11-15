import debugFn from 'debug'
import { setTimeout } from 'timers/promises'
import type { Connection } from './types.js'

const debug = debugFn('integreat:transporter:redis')

export default async function disconnect(
  connection: Connection | null,
): Promise<void> {
  if (connection && connection.status === 'ok' && connection.redisClient) {
    debug(
      `Disconnect Redis client if still ready [isOpen=${connection.redisClient.isOpen}, isReady=${connection.redisClient.isReady}]`,
    )
    if (connection.redisClient.isReady) {
      debug('Attempting to disconnect Redis client calling redisClient.quit()')
      try {
        const timeoutMessage = 'redisClient.quit() timed out'
        const result = await Promise.race([
          connection.redisClient.quit(),
          // TODO: Make this timeout configurable too maybe?
          setTimeout(100, timeoutMessage),
        ])
        if (result === timeoutMessage) {
          debug(
            'Timed out while calling redisClient.quit(), the connection is already closed',
          )
        }
      } catch (error) {
        debug(
          'Failed to call redisClient.quit(), ignoring the error as the connection is already closed:',
          error,
        )
      }
    }
    debug('Setting connection.redisClient to null')
    connection.redisClient = null
  }
}
