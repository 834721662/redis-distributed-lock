import lock.Lock;
import lock.LockException;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

/**
 * @author zj
 * @since 2018/11/17
 */
public class RedisLock implements Lock {

    private Jedis jedis;
    private String key;
    private String value;
    private boolean isLock;

    public static long DEFAULT_TIMEOUT = 5_000_000_000l;
    public static int DEFAULT_EXPIRE = 10;

    public RedisLock(Jedis jedis, String key) {
        this.jedis = jedis;
        this.key = key;
    }

    @Override
    public void lock() throws LockException {
        lock(DEFAULT_TIMEOUT, DEFAULT_EXPIRE);
    }

    @Override
    public void lock(long timeoutInNa, int expireInSec) throws LockException {
        boolean b = tryLock(timeoutInNa, expireInSec);
        if (!b) {
            throw new LockException("can't lock.");
        }
    }

    private boolean tryLock(long timeoutInNa, int expireInSec) {
        long startTime = System.nanoTime();
        try {
            while ((System.nanoTime() - startTime) < timeoutInNa) {
                long currentTimeMillis = System.currentTimeMillis();
                String lockValue = String.valueOf(currentTimeMillis + expireInSec * 1000 + 1);

                //将key对应的值设置为 lockValue，当且仅当key不存在的时候,时间复杂度为o1
                Long setnx = jedis.setnx(key, lockValue);
                if (setnx == 1) {
                    handleLockSuccess(lockValue, expireInSec);
                    return true;
                }

                String currentValueStr = jedis.get(key);
                if (null != currentValueStr && Long.parseLong(currentValueStr) < currentTimeMillis) {
                    String oldValueStr = jedis.getSet(key, lockValue);

                    if (currentValueStr.equals(oldValueStr)) {
                        handleLockSuccess(lockValue, expireInSec);
                        return true;
                    }
                }

                TimeUnit.MILLISECONDS.sleep(10);
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("lock failed.", e);
        }
    }

    /**
     * 如果锁成功，设置对应的超时时间，并且更新本地状态
     * @param lockValue
     * @param expireInSec
     */
    private void handleLockSuccess(String lockValue, int expireInSec) {
        jedis.expire(key, expireInSec);

        this.isLock = true;
        this.value = lockValue;
    }

    @Override
    public void unlock() {
        if (isLock) {
            if (null != this.value && value.equals(jedis.get(key))) {
                jedis.del(key);
            }
            isLock = false;
        }
    }


}
