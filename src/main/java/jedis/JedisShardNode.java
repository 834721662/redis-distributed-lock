package jedis;

import java.io.Serializable;

/**
 * 共享节点，序列化服务
 * @author zj
 * @since 2018/11/16
 */
public class JedisShardNode implements Serializable {

    private int id;

    private String host;

    private int port;

    private int timeout;

    private String pwd;

    public static final int DEFAULT_TIMEOUT = 1000;

    public JedisShardNode(int id, String host, int port, int timeout, String pwd) {
        this.id = id;
        this.host = host;
        this.pwd = pwd;
        this.port = port;
        this.timeout = timeout;
    }

    public JedisShardNode(int id, String host, int port) {
        this(id, host, port, DEFAULT_TIMEOUT);
    }

    public JedisShardNode(int id, String host, int port, int timeout) {
        this(id, host, port, timeout, null);
    }

    public JedisShardNode(int id, String host, int port, String pwd) {
        this(id, host, port, DEFAULT_TIMEOUT, pwd);
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (this == o) return true;

        JedisShardNode jedisShardNode = (JedisShardNode) o;

        if (id != jedisShardNode.id) return false;
        if (port != jedisShardNode.port) return false;
        if (host != null ? !host.equals(jedisShardNode.host) : jedisShardNode.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }
}
