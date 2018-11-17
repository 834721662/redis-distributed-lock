package jedis;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author zj
 * @since 2018/11/17
 */
public class NodeIdSorting implements ShardNodeGenStrategy.Sorting {

    @Override
    public List<JedisShardNode> sort(List<JedisShardNode> nodes) {
        Collections.sort(nodes, new Comparator<JedisShardNode>() {
            @Override
            public int compare(JedisShardNode o1, JedisShardNode o2) {
                return o1.getId() - o2.getId();
            }
        });
        return Collections.unmodifiableList(nodes);
    }

}
