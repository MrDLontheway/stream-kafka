package com.wxstc.dl.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.util.Set;

/**
 * redis客户端单机版实现类
 * @author L
 *
 */
public class JedisClientSingle implements JedisClient,Serializable {
	JedisClientSingle(){
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxWaitMillis(100000);
		config.setMaxIdle(10);config.setMaxTotal(100);
		//JedisPool j = new JedisPool(config,"47.94.196.195",6378);
		jedisPool = new JedisPool(config,"47.94.196.195",6378,10000,"heiwofuwuqisiquanjiaCNM");
	}
	private JedisPool jedisPool;

	public String set(String key, String value) {
		Jedis jedis = jedisPool.getResource();
		String result = jedis.set(key, value);
		jedis.close();
		return result;
	}

	public String get(String key) {
		Jedis jedis = jedisPool.getResource();
		String result = jedis.get(key);
		jedis.close();
		return result;
	}

	public Long hset(String key, String item, String value) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.hset(key, item, value);
		jedis.close();
		return result;
	}

	public String hget(String key, String item) {
		Jedis jedis = jedisPool.getResource();
		String result = jedis.hget(key, item);
		jedis.close();
		return result;
	}

	public Long incr(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.incr(key);
		jedis.close();
		return result;
	}

	public Long decr(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.decr(key);
		jedis.close();
		return result;
	}

	public Long expire(String key, int second) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.expire(key, second);
		jedis.close();
		return result;
	}

	public Long ttl(String key) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.ttl(key);
		jedis.close();
		return result;
	}

	public Long hdel(String key, String item) {
		Jedis jedis = jedisPool.getResource();
		Long result = jedis.hdel(key, item);
		jedis.close();
		return result;
	}

	public Set<String> keys(String pattern){
		Jedis jedis = jedisPool.getResource();
		Set<String> result = jedis.keys(pattern);
		jedis.close();
		return result;
	}

	public Set<String> hkeys(String pattern){
		Jedis jedis = jedisPool.getResource();
		Set<String> result = jedis.hkeys(pattern);
		jedis.close();
		return result;
	}
}
