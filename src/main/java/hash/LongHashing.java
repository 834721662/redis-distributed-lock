package hash;

/**
 * @author zj
 * @since 2018/11/16
 */
public class LongHashing implements Hashing {

    @Override
    public long hash(String key) {
        return Long.parseLong(key);
    }

}
