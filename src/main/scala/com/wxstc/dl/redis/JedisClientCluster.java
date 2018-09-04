package com.wxstc.dl.redis;

import redis.clients.jedis.JedisCluster;

import java.util.Set;

/**
 * redis集群版的实现类
 * @author L
 *
 */
public class JedisClientCluster implements JedisClient {

	private JedisCluster jedisCluster;
	
	public String set(String key, String value) {
		return jedisCluster.set(key, value);
	}

	public String get(String key) {
		return jedisCluster.get(key);
	}

	public Long hset(String key, String item, String value) {
		return jedisCluster.hset(key, item, value);
	}

	public String hget(String key, String item) {
		return jedisCluster.hget(key, item);
	}

	public Long incr(String key) {
		return jedisCluster.incr(key);
	}

	public Long decr(String key) {
		return jedisCluster.decr(key);
	}

	public Long expire(String key, int second) {
		return jedisCluster.expire(key, second);
	}

	public Long ttl(String key) {
		return jedisCluster.ttl(key);
	}

	public Long hdel(String key, String item) {
		return jedisCluster.hdel(key, item);
	}

	public Set<String> keys(String pattern) {
		return jedisCluster.hkeys(pattern);
	}

	public Set<String> hkeys(String pattern) {
		return jedisCluster.hkeys(pattern);
	}

}
