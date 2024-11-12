import debugFn from 'debug'
import type { Connection } from './types.js'

const debug = debugFn('integreat:transporter:redis')

export default async function disconnect(
  connection: Connection | null,
): Promise<void> {
  if (connection && connection.status === 'ok' && connection.redisClient) {
    debug('Disconnect Redis client if still open')
    if (connection.redisClient.isOpen) {
      try {
        await connection.redisClient.quit()
      } catch (error) {
        debug('Failed to call redisClient.quit(), ignoring the error:', error)
      }
    }
    connection.redisClient = null
  }
}
