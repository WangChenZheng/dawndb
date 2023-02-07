package dawndb.tx.concurrency;

import dawndb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 17:05
 * @Version: 1.0
 * @Description: 事务的并发管理器。每个事务都有自己的并发管理器。
 * 并发管理器跟踪事务当前拥有哪些锁，并根据需要与全局锁表进行交互。
 */


public class ConcurrencyMgr {

    private static LockTable locktbl = new LockTable();
    private Map<BlockId, String> locks = new HashMap<BlockId, String>();

    /**
     * 在块上获得一个SLock。
     * 如果事务当前在该块上没有锁，该方法将向锁表请求一个SLock。
     */
    public void sLock(BlockId blk) {
        if (locks.get(blk) == null) {
            locktbl.sLock(blk);
            locks.put(blk, "S");
        }
    }

    /**
     * 在块上获得一个XLock。
     * 如果事务在该块上没有XLock，那么该方法首先在该块上获得一个SLock，然后将其升级为XLock。
     */
    public void xLock(BlockId blk) {
        if (!hasXLock(blk)) {
            sLock(blk);
            locktbl.xLock(blk);
            locks.put(blk, "X");
        }
    }

    /**
     * 通过请求锁表解锁每个锁来释放所有锁。
     */
    public void release() {
        for (BlockId blk : locks.keySet()) {
            locktbl.unlock(blk);
        }
        locks.clear();
    }

    private boolean hasXLock(BlockId blk) {
        String locktype = locks.get(blk);
        return locktype != null && locktype.equals("X");
    }

}
