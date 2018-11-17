package lock;

/**
 * @author zj
 * @since 2018/11/17
 */
public interface Lock {

    public void lock() throws LockException;

    public void lock(long timeout, int expire) throws LockException;

    public void unlock();

}
