package jedis;

import java.util.List;

/**
 * 策略
 * @author zj
 * @since 2018/11/16
 */
public interface ShardNodeGenStrategy {

    List<JedisShardNode> getNodes();

    interface Sorting {

        List<JedisShardNode> sort(List<JedisShardNode> nodes);
    }

}
