package hash;

/**
 * @author zj
 * @since 2018/11/16
 */
public class SignlessJavaHashing implements Hashing {

    @Override
    public long hash(String key) {
        return key.hashCode() & 0x7fffffff;
    }


}
