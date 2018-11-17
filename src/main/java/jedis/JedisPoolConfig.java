package jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Set the default parameter configuration
 * @author zj
 * @since 2018/11/16
 */
public class JedisPoolConfig extends GenericObjectPoolConfig {

    public JedisPoolConfig() {
        setTestWhileIdle(true);
        setMinEvictableIdleTimeMillis(300000);
        setTimeBetweenEvictionRunsMillis(60000);
        setNumTestsPerEvictionRun(-1);
    }
}
