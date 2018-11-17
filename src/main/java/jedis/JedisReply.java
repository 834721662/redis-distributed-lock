package jedis;

import redis.clients.jedis.JedisShardInfo;

/**
 * @author zj
 * @since 2018/11/16
 */
public class JedisReply<T> implements Response {

    private T result;

    private JedisShardInfo jedisShardInfo;

    public JedisReply(T result, JedisShardInfo jedisShardInfo) {
        this.result = result;
        this.jedisShardInfo = jedisShardInfo;
    }

    @Override
    public T getResult() {
        return result;
    }

    public JedisShardInfo getJedisShardInfo() {
        return jedisShardInfo;
    }
}
