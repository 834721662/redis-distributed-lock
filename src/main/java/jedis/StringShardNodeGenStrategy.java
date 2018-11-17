package jedis;

import java.util.ArrayList;
import java.util.List;

/**
 * 通过配置生成shard node策略类，支持该配置：${id}:${host}:${port}:${password}
 * password非必须
 * @author zj
 * @since 2018/11/17
 */
public class StringShardNodeGenStrategy implements ShardNodeGenStrategy {

    private String config;
    private String password;
    private Sorting sorting;
    private int timeout = JedisShardNode.DEFAULT_TIMEOUT;

    public StringShardNodeGenStrategy(String config) {
        this.config = config;
    }

    public StringShardNodeGenStrategy(String config, String password) {
        this.config = config;
        this.password = password;
    }

    public StringShardNodeGenStrategy(String config, Sorting sorting) {
        this.config = config;
        this.sorting = sorting;
    }

    public StringShardNodeGenStrategy(String config, String password, Sorting sorting) {
        this.config = config;
        this.password = password;
        this.sorting = sorting;
    }

    public StringShardNodeGenStrategy(String config, String password, Sorting sorting, int timeout) {
        this.config = config;
        this.password = password;
        this.sorting = sorting;
        this.timeout = timeout;
    }

    @Override
    public List<JedisShardNode> getNodes() {
        List<JedisShardNode> list = new ArrayList<>();
        String[] split = config.split(",");
        for (String s : split) {
            String[] split1 = s.split(":");
            if (split1.length == 3) {
                list.add(new JedisShardNode(Integer.parseInt(split1[0]), split1[1], Integer.parseInt(split1[2]), timeout, password));
            } else if (split1.length == 4) {
                list.add(new JedisShardNode(Integer.parseInt(split1[0]), split1[1], Integer.parseInt(split1[2]), timeout, split1[3]));
            }
        }
        return sorting == null ? list : sorting.sort(list);
    }


}
