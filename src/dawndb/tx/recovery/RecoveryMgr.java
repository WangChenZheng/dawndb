package dawndb.tx.recovery;

import dawndb.buffer.Buffer;
import dawndb.buffer.BufferMgr;
import dawndb.file.BlockId;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @Author: WangChen
 * @Date: 2023/2/15 10:36
 * @Version: 1.0
 * @Description: 恢复管理器。每个事务都有自己的恢复管理器。
 */


public class RecoveryMgr {

    private LogMgr lm;
    private BufferMgr bm;
    private Transaction tx;
    private int txnum;

    /**
     * 为指定的事务创建恢复管理器。
     * @param tx 事务
     * @param txnum 事务ID
     * @param lm 日志管理器
     * @param bm 缓冲区管理器
     */
    public RecoveryMgr(Transaction tx, int txnum, LogMgr lm, BufferMgr bm) {
        this.tx = tx;
        this.txnum = txnum;
        this.lm = lm;
        this.bm = bm;
        StartRecord.writeToLog(lm, txnum);
    }

    /**
     * 将提交记录写入日志，并将其刷新到磁盘。
     */
    public void commit() {
        bm.flushAll(txnum);
        int lsn = CommitRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    /**
     * 将回滚记录写入日志并将其刷新到磁盘。
     */
    public void rollback() {
        doRollback();
        bm.flushAll(txnum);
        int lsn = RollbackRecord.writeToLog(lm, txnum);
        lm.flush(lsn);
    }

    /**
     * 从日志中恢复未完成的事务，然后将静止检查点记录写入日志并刷新它。
     */
    public void recover() {
        doRecover();
        bm.flushAll(txnum);
        int lsn = CheckpointRecord.writeToLog(lm);
        lm.flush(lsn);
    }

    public int setInt(Buffer buff, int offset, int newval) {
        int oldval = buff.contents().getInt(offset);
        BlockId blk = buff.block();
        return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }

    public int setString(Buffer buff, int offset, String newval) {
        String oldval = buff.contents().getString(offset);
        BlockId blk = buff.block();
        return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
    }

    private void doRollback() {
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if (rec.txNumber() == txnum) {
                if (rec.op() == LogRecord.START) {
                    return;
                }
                rec.undo(tx);
            }
        }
    }

    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if (rec.op() == LogRecord.CHECKPOINT) {
                return;
            }
            if (rec.op() == LogRecord.COMMIT || rec.op() == LogRecord.ROLLBACK) {
                finishedTxs.add(rec.txNumber());
            }
            else if (!finishedTxs.contains(rec.txNumber())) {
                rec.undo(tx);
            }
        }
    }

}
