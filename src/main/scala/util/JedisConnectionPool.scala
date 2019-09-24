package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool {
  val config = new JedisPoolConfig()
  config.setMaxIdle(10)
  config.setMaxTotal(20)
  private val pool = new JedisPool(config,"hadoop01",6379,10000)

  def getConnection():Jedis={
    pool.getResource
  }

}
