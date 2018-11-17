package jedis;

/**
 * @author zj
 * @since 2018/11/16
 */
public interface Response<T> {

    T getResult();

}
