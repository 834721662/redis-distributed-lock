package jedis;

/**
 * @author zj
 * @since 2018/11/16
 */
public class JedisShardReply<T> implements Response {

    private T result;

    private JedisShardNode node;

    public JedisShardReply(T result, JedisShardNode node) {
        this.result = result;
        this.node = node;
    }

    @Override
    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public JedisShardNode getNode() {
        return node;
    }

    public void setNode(JedisShardNode node) {
        this.node = node;
    }

}
