package dawndb.buffer;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.log.LogMgr;

/**
 * @Author: WangChen
 * @Date: 2023/1/10 11:59
 * @Version: 1.0
 * @Description:
 * 缓冲区管理器
 * bufferPool: 缓冲池
 * numAvailable: 可用块的数量
 * MAX_TIME: 最大等待时间
 */


public class BufferMgr {

    private Buffer[] bufferPool;
    private int numAvailable;
    private static final long MAX_TIME = 10000;

    /**
     * 初始化缓冲区管理器
     * @param numbuffs 缓冲区个数
     */
    public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        bufferPool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        // 初始化缓冲区
        for (int i=0; i< numbuffs; i++) {
            bufferPool[i] = new Buffer(fm, lm);
        }
    }

    /**
     * 返回当前可用缓冲区个数
     */
    public synchronized int available() {
        return numAvailable;
    }

    /**
     * 将被指定事务修改的缓冲区写回内存
     */
    public synchronized void flushAll(int txnum) {
        for (Buffer buff : bufferPool) {
            if (buff.modifyingTx() == txnum) {
                buff.flush();
            }
        }
    }

    /**
     * 取消引用某个缓冲区。如果它的引用数为零，那么通知任何等待线程。
     */
    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    /**
     * 将缓冲区固定到指定的块上，可能会等待缓冲区可用。
     * 若在某段时间内没有可用缓冲区则抛出{@link BufferAbortException}异常
     */
    public synchronized Buffer pin(BlockId blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = tryToPin(blk);
            while (buff == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buff = tryToPin(blk);
            }
            if (buff == null) {
                throw new BufferAbortException();
            }
            return buff;
        }
        catch(InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    /**
     * 等待时间是否超过某个值
     */
    private boolean waitingTooLong(long starttime) {
        return System.currentTimeMillis() - starttime > MAX_TIME;
    }

    /**
     * 尝试将缓冲区固定到指定的块。如果已经有一个缓冲区分配给该块，
     * 那么该缓冲区将被使用;否则，将从池中选择一个未固定的缓冲区。
     * 如果没有可用缓冲区，则返回空值。
     */
    private Buffer tryToPin(BlockId blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null) {
                return null;
            }
            buff.assignToBlock(blk);
        }
        if (!buff.isPinned()) {
            numAvailable--;
        }
        buff.pin();
        return buff;
    }

    /**
     * 找到被固定到指定块的缓冲区
     */
    private Buffer findExistingBuffer(BlockId blk) {
        for (Buffer buff : bufferPool) {
            BlockId b = buff.block();
            if (b != null && b.equals(blk)) {
                return buff;
            }
        }
        return null;
    }

    /**
     * 找到未被固定的缓冲区
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : bufferPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }
}
