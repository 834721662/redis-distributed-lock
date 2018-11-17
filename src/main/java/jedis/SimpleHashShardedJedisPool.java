package jedis;

import hash.Hashing;
import hash.JavaHashing;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zj
 * @since 2018/11/17
 */
public class SimpleHashShardedJedisPool {


    public static Hashing DEFAULT_HASHING = new JavaHashing();
    private List<JedisShardNode> jedisShardNodeList;
    private int modSize;
    private Map<JedisShardNode, JedisPool> jedisPoolMap = new HashMap<>();
    private Hashing hashing;

    public SimpleHashShardedJedisPool(ShardNodeGenStrategy nodeGenStrategy, JedisPoolConfig config) {
        this(nodeGenStrategy, config, DEFAULT_HASHING);
    }

    public SimpleHashShardedJedisPool(ShardNodeGenStrategy nodeGenStrategy, JedisPoolConfig config, Hashing hash) {
        this.jedisShardNodeList = nodeGenStrategy.getNodes();
        this.modSize = jedisShardNodeList.size();
        this.hashing = hash;
        for (JedisShardNode jedisShardNode : jedisShardNodeList) {
            jedisPoolMap.put(jedisShardNode, new JedisPool(config, jedisShardNode.getHost(), jedisShardNode.getPort(), jedisShardNode.getTimeout(), jedisShardNode.getPwd()));
        }
    }

    public int getNumActive() {
        int sum = 0;
        for (JedisPool jedisPool : jedisPoolMap.values()) {
            sum += jedisPool.getNumActive();
        }
        return sum;
    }

    public JedisShardReply<String> get(String key) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.get(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<String> hgetWithDb(String key, String field, int db) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.hget(key, field);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<String> hget(String key, String field) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.hget(key, field);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Map<String, String>> hgetall(String key) {
        Jedis jedis = null;
        Map<String, String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.hgetAll(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Map<String, String>> hgetallWithDb(String key, int db) {
        Jedis jedis = null;
        Map<String, String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.hgetAll(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<List<String>> hmget(String key, String... fields) {
        Jedis jedis = null;
        List<String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.hmget(key, fields);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<List<String>> hmgetWithDb(int db, String key, String... fields) {
        Jedis jedis = null;
        List<String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.hmget(key, fields);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<List<byte[]>> hmget(byte[] key, byte[]... fields) {
        Jedis jedis = null;
        List<byte[]> result = null;
        JedisPool pool = getShardedPool(new String(key));
        try {
            jedis = pool.getResource();
            result = jedis.hmget(key, fields);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(new String(key)));
    }

    public JedisShardReply<List<byte[]>> hmgetWithDb(int db, byte[] key, byte[]... fields) {
        Jedis jedis = null;
        List<byte[]> result = null;
        JedisPool pool = getShardedPool(new String(key));
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.hmget(key, fields);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(new String(key)));
    }

    public JedisShardReply<Long> setnx(String key, String value) {
        Jedis jedis = null;
        long result;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.setnx(key, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> incrBy(String key, long incr) {
        Jedis jedis = null;
        long result;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.incrBy(key, incr);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hset(String key, String field, String value) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.hset(key, field, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hsetWithDb(String key, String field, String value, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.hset(key, field, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hmset(String key, Map<String, String> hash) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            jedis.hmset(key, hash);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hmsetWithDb(String key, Map<String, String> hash, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            jedis.hmset(key, hash);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hincrBy(String key, String field, long value) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            jedis.hincrBy(key, field, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hincrByWithDb(String key, String field, long value, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            jedis.hincrBy(key, field, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> hdelWithDb(String key, String field, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            jedis.hdel(key, field);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<String> set(String key, String value) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.set(key, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Boolean> getBit(String key, long offset) {
        Jedis jedis = null;
        Boolean result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.getbit(key, offset);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Boolean> setBit(String key, long offset, boolean value) {
        Jedis jedis = null;
        Boolean result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.setbit(key, offset, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<String> setWithDb(String key, String value, int db) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.set(key, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<String> getWithDb(String key, int db) {
        Jedis jedis = null;
        String result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.get(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Boolean> existsWithDb(String key, int db) {
        Jedis jedis = null;
        Boolean result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0) {
                jedis.select(db);
            }
            result = jedis.exists(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> expireAt(String key, long unixTime) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.expireAt(key, unixTime);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> expire(String key, int seconds) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.expire(key, seconds);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> expiredWithDb(String key, int seconds, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.expire(key, seconds);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> expiredAtWithDb(String key, long unixTime, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.expireAt(key, unixTime);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> del(String key) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.del(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> delWithDb(String key, int db) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.del(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> sadd(String key, String... members) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.sadd(key, members);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> saddWithDb(String key, int db, String... members) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.sadd(key, members);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Set<String>> smembers(String key) {
        Jedis jedis = null;
        Set<String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            result = jedis.smembers(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Set<String>> smembersWithDb(String key, int db) {
        Jedis jedis = null;
        Set<String> result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.smembers(key);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisShardReply<Long> sremWithDb(String key, int db, String... members) {
        Jedis jedis = null;
        Long result = null;
        JedisPool pool = getShardedPool(key);
        try {
            jedis = pool.getResource();
            if (db > 0)
                jedis.select(db);
            result = jedis.srem(key, members);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return new JedisShardReply<>(result, getNode(key));
    }

    public JedisPool getShardedPool(String key) {
        JedisShardNode jedisShardNode = getNode(key);
        return jedisPoolMap.get(jedisShardNode);
    }

    public Jedis getResource(String key) {
        JedisPool pool = getShardedPool(key);
        return pool.getResource();
    }

    public void returnResource(Jedis jedis) {
        jedis.close();
    }

    private JedisShardNode getNode(String key) {
        long hash = hashing.hash(key);
        int index = (int) (Math.abs(hash) % modSize);
        return this.jedisShardNodeList.get(index);
    }

    public int getNodeIndex(String key) {
        long hash = hashing.hash(key);
        return (int) (Math.abs(hash) % modSize);
    }

    public Jedis getResourceByIndex(int index) {
        JedisShardNode jedisShardNode = jedisShardNodeList.get(index);
        JedisPool jedisPool = jedisPoolMap.get(jedisShardNode);
        return jedisPool.getResource();
    }

    public JedisPool getShardedPoolByIndex(int index) {
        return jedisPoolMap.get(jedisShardNodeList.get(index));
    }

    public void close() {
        for (Map.Entry<JedisShardNode, JedisPool> entry : jedisPoolMap.entrySet()) {
            JedisPool jedisPool = entry.getValue();
            jedisPool.close();
        }
    }

}
