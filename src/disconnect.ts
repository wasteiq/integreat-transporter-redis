import debugFn from 'debug'
import type { Connection } from './types.js'

const debug = debugFn('integreat:transporter:redis')

export default async function disconnect(
  connection: Connection | null,
): Promise<void> {
  if (connection && connection.status === 'ok' && connection.redisClient) {
    debug('Disconnect Redis client if still open')
    if (connection.redisClient.isOpen) {
      await connection.redisClient.quit()
    }
    connection.redisClient = null
  }
}
