import jedis.SimpleHashShardedJedisPool;
import lock.LockException;
import lock.LockKeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author zj
 * @since 2018/11/17
 */
public class MultiRedisLock {

    private final SimpleHashShardedJedisPool jedisPool;
    /** lock信息存储的对应的redis db */
    private final int lockDBIndex;
    //锁失效时间 单位秒
    /** 锁失效时间 单位秒 */
    private final int lockExpireSeconds;

    private static final Logger logger = LoggerFactory.getLogger(MultiRedisLock.class);

    public MultiRedisLock(SimpleHashShardedJedisPool jedisPool, int lockDBIndex, int lockExpireSeconds) {
        this.jedisPool = jedisPool;
        this.lockDBIndex = lockDBIndex;
        this.lockExpireSeconds = lockExpireSeconds;
    }

    /**
     * redis 多锁操作
     * @param lockKeySet 多锁相关键值
     * @param func 锁成功后执行的函数
     * @param timeoutInNa 锁尝试时间
     * @param <T>
     * @throws Exception
     */
    public <T extends IWithLockFunc> void handleWithLock(Set<String> lockKeySet, T func, long timeoutInNa) throws
            Exception {
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) < timeoutInNa) {
            try {
                handleWithLock(lockKeySet, func);
                return;
            } catch (Throwable t) {
                if (t instanceof LockException) {
                    TimeUnit.MILLISECONDS.sleep(20);
                } else {
                    throw t;
                }
            }
        }

        throw new RuntimeException("Lock failed ...");
    }

    private <T extends IWithLockFunc> void handleWithLock(Set<String> lockKeySet, T func) throws Exception {
        List<LockKeyValue> lockKeyValueList = new ArrayList<>();
        try {
            for (String lockKey : lockKeySet) {
                String lockValue = String.valueOf(System.currentTimeMillis() + this.lockExpireSeconds * 1000 + 1);

                try (Jedis jedis = this.jedisPool.getResource(lockKey)) {
                    jedis.select(this.lockDBIndex);
                    Long lockResult = jedis.setnx(lockKey, lockValue);

                    //锁成功，需要添加失效时间
                    lockCheck:
                    if (1L == lockResult.longValue()) {
                        jedis.expire(lockKey, this.lockExpireSeconds);
                    } else { //锁失败
                        String currentValueStr = jedis.get(lockKey);
                        if (null != currentValueStr && !"".equals(currentValueStr) && Long.parseLong
                                (currentValueStr) < System.currentTimeMillis()) {
                            String oldValueStr = jedis.getSet(lockKey, lockValue);

                            if (currentValueStr.equals(oldValueStr)) {
                                jedis.expire(lockKey, this.lockExpireSeconds);
                                break lockCheck;
                            }
                        }

                        throw new LockException("Lock failed ...");
                    }

                    lockKeyValueList.add(new LockKeyValue(lockKey, lockValue));
                }
            }
        } catch (Throwable t) {
            unlock(lockKeyValueList);
            throw t;
        }

        try {
            func.exec();
        } finally {
            unlock(lockKeyValueList);
        }
    }

    private void unlock(List<LockKeyValue> lockKeyValueList) {
        for (LockKeyValue lockKv : lockKeyValueList) {
            try (Jedis jedis = this.jedisPool.getResource(lockKv.key)) {
                jedis.select(this.lockDBIndex);

                String value = jedis.get(lockKv.key);
                if (lockKv.value.equals(value) && System.currentTimeMillis() < Long.parseLong(value)) {
                    jedis.del(lockKv.key);
                }
            } catch (Exception e) {
                logger.warn("Handle unlock failed ", e);
            }
        }
    }

}
