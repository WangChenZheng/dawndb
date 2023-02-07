package dawndb.tx.concurrency;

import dawndb.file.BlockId;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 17:56
 * @Version: 1.0
 * @Description: 锁定表，它提供了锁定和解锁块的方法。
 * 如果事务请求的锁与现有锁发生冲突，则该事务将被放到等待列表中。
 * 所有区块只有一个等待列表。当一个块上的最后一个锁被解锁时，所
 * 有事务将从等待列表中删除并重新调度。如果其中一个事务发现它正
 * 在等待的锁仍然锁定，它将把自己放回到等待列表中。
 */

public class LockTable {

    private static final long MAX_TIME = 10000;
    private Map<BlockId, Integer> locks = new HashMap<BlockId, Integer>();

    /**
     * 在指定的块上授予共享锁。如果在调用该方法时存在互斥锁，
     * 则调用线程将被放置在等待列表中，直到锁被释放。如果线
     * 程在等待列表中停留了一定的时间(10秒)后仍存在互斥锁，
     * 则抛出异常。
     * @param blk
     */
    public synchronized void sLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXLock(blk) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasXLock(blk)) {
                throw new LockAbortException();
            }
            int val = getLockVal(blk);
            locks.put(blk, val+1);
        } catch(InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * 在指定的块上授予互斥锁。如果在调用方法时存在任何类型的锁，
     * 那么调用线程将被放置在等待列表中，直到锁被释放。如果线程
     * 在等待列表中停留了一定的时间(10秒)后仍存在锁，则抛出异常。
     */
    public synchronized void xLock(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(blk) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasOtherSLocks(blk)) {
                throw new LockAbortException();
            }
            locks.put(blk, -1);
        }
        catch(InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * 释放指定块上的锁。如果此锁是该块上的最后一个锁，
     * 则会通知正在等待的事务。
     */
    synchronized void unlock(BlockId blk) {
        int val = getLockVal(blk);
        if (val > 1) {
            locks.put(blk, val-1);
        } else {
            locks.remove(blk);
            notifyAll();
        }
    }

    private boolean hasOtherSLocks(BlockId blk) {
        return getLockVal(blk) > 1;
    }

    private int getLockVal(BlockId blk) {
        Integer ival = locks.get(blk);
        return (ival == null) ? 0 : ival;
    }

    private boolean hasXLock(BlockId blk) {
        return getLockVal(blk) < 0;
    }

    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

}
