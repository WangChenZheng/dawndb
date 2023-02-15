package dawndb.tx;

import dawndb.buffer.Buffer;
import dawndb.buffer.BufferMgr;
import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.concurrency.ConcurrencyMgr;
import dawndb.tx.recovery.RecoveryMgr;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 12:15
 * @Version: 1.0
 * @Description:
 * 为客户端提供事务管理，确保所有事务都是可序列化的、可恢复的，并且总体上满足ACID属性。
 */


public class Transaction {

    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurMgr;
    private BufferMgr bm;
    private FileMgr fm;
    private int txnum;
    private BufferList buffers;

    /**
     * 创建一个新事务及其相关的恢复和并发管理器。
     */
    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
        this.fm = fm;
        this.bm = bm;
        txnum = nextTxNumber();
        recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
        concurMgr = new ConcurrencyMgr();
        buffers = new BufferList(bm);
    }

    /**
     * 提交当前事务。刷新所有修改的缓冲区(及其日志记录)，
     * 将提交记录写入并刷新到日志中，释放所有锁，并解除任何缓冲区的固定。
     */
    public void commit() {
        recoveryMgr.commit();
        System.out.println("transaction " + txnum + " committed");
        concurMgr.release();
        buffers.unpinAll();
    }

    /**
     * 回滚当前事务。撤消所有修改的值，刷新这些缓冲区，
     * 写入回滚记录并将其刷新到日志中，释放所有锁，并解除任何固定缓冲区。
     */
    public void rollback() {
        recoveryMgr.rollback();
        System.out.println("transaction " + txnum + " rolled back");
        concurMgr.release();
        buffers.unpinAll();
    }

    /**
     * 刷新所有修改的缓冲区。然后查看日志，回滚所有未提交的事务。
     * 最后，向日志中写入一个静态检查点记录。在系统启动期间，在用户事务开始之前调用此方法。
     */
    public void recover() {
        bm.flushAll(txnum);
        recoveryMgr.recover();
    }

    public void pin(BlockId blk) {
        buffers.pin(blk);
    }

    public void unpin(BlockId blk) {
        buffers.unpin(blk);
    }

    /**
     * 返回存储在指定块的指定偏移量处的整数值。
     * 该方法首先获取块上的SLock，然后调用缓冲区来检索值。
     */
    public int getInt(BlockId blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = buffers.getBuffer(blk);
        return buff.contents().getInt(offset);
    }

    /**
     * 返回存储在指定块的指定偏移处的字符串值。
     * 该方法首先获取块上的SLock，然后调用缓冲区来检索值。
     */
    public String getString(BlockId blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = buffers.getBuffer(blk);
        return buff.contents().getString(offset);
    }

    /**
     * 在指定块的指定偏移量处存储一个整数。该方法首先获取块上的XLock。
     * 然后，它在该偏移量处读取当前值，将其放入更新日志记录中，并将该记录写入日志。
     * 最后，它调用缓冲区来存储值，传入日志记录的LSN和事务的id。
     */
    public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
        concurMgr.xLock(blk);
        Buffer buff = buffers.getBuffer(blk);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryMgr.setInt(buff, offset, val);
        }
        Page p = buff.contents();
        p.setInt(offset, val);
        buff.setModified(txnum, lsn);
    }

    /**
     * 在指定块的指定偏移处存储字符串。该方法首先获取块上的XLock。
     * 然后，它在该偏移量处读取当前值，将其放入更新日志记录中，并将该记录写入日志。
     * 最后，它调用缓冲区来存储值，传入日志记录的LSN和事务的id。
     */
    public void setString(BlockId blk, int offset, String val, boolean okToLog) {
        concurMgr.xLock(blk);
        Buffer buff = buffers.getBuffer(blk);
        int lsn = -1;
        if (okToLog) {
            lsn = recoveryMgr.setString(buff, offset, val);
        }
        Page p = buff.contents();
        p.setString(offset, val);
        buff.setModified(txnum, lsn);
    }

    /**
     * 在指定文件的末尾追加一个新块，并返回对该块的引用。
     * 在执行追加操作之前，该方法首先在“文件的末尾新块”上获得一个XLock。
     * @return
     */
    public BlockId append(String filename) {
        BlockId dummyblk = new BlockId(filename, END_OF_FILE);
        concurMgr.xLock(dummyblk);
        return fm.append(filename);
    }

    public int blockSize() {
        return fm.blockSize();
    }

    public int availableBuffs() {
        return bm.available();
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        return nextTxNum;
    }
}
