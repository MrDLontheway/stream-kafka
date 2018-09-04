package com.wxstc.dl.redis

import java.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

class JedisSingle extends Serializable {
  private var jedisPool = {
    val config = new JedisPoolConfig
    config.setMaxWaitMillis(100000)
    config.setMaxIdle(10)
    config.setMaxTotal(100)
    //JedisPool j = new JedisPool(config,"47.94.196.195",6378);
    new JedisPool(config, "47.94.196.195", 6378, 10000, "heiwofuwuqisiquanjiaCNM")}

  def set(key: String, value: String): String = {
    val jedis = jedisPool.getResource
    val result = jedis.set(key, value)
    jedis.close()
    result
  }

  def get(key: String): String = {
    val jedis = jedisPool.getResource
    val result = jedis.get(key)
    jedis.close()
    result
  }

  def hset(key: String, item: String, value: String): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.hset(key, item, value)
    jedis.close()
    result
  }

  def hget(key: String, item: String): String = {
    val jedis = jedisPool.getResource
    val result = jedis.hget(key, item)
    jedis.close()
    result
  }

  def incr(key: String): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.incr(key)
    jedis.close()
    result
  }

  def decr(key: String): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.decr(key)
    jedis.close()
    result
  }

  def expire(key: String, second: Int): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.expire(key, second)
    jedis.close()
    result
  }

  def ttl(key: String): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.ttl(key)
    jedis.close()
    result
  }

  def hdel(key: String, item: String): Long = {
    val jedis = jedisPool.getResource
    val result = jedis.hdel(key, item)
    jedis.close()
    result
  }

  def keys(pattern: String): util.Set[String] = {
    val jedis = jedisPool.getResource
    val result = jedis.keys(pattern)
    jedis.close()
    result
  }

  def hkeys(pattern: String): util.Set[String] = {
    val jedis = jedisPool.getResource
    val result = jedis.hkeys(pattern)
    jedis.close()
    result
  }
}

object JedisSingle{
  val jedisPool = {
    val config = new JedisPoolConfig
    config.setMaxWaitMillis(100000)
    config.setMaxIdle(10)
    config.setMaxTotal(100)
    //JedisPool j = new JedisPool(config,"47.94.196.195",6378);
    new JedisPool(config, "47.94.196.195", 6378, 10000, "heiwofuwuqisiquanjiaCNM")}
}
