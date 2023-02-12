package dawndb.tx.recovery;

import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 13:01
 * @Version: 1.0
 * @Description: COMMIT类型日志记录
 */


public class CommitRecord implements LogRecord{

    private int txnum;

    public CommitRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
    }

    @Override
    public int op() {
        return COMMIT;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    /**
     * 什么都不做
     * @param tx 正在执行撤销操作的事务
     */
    @Override
    public void undo(Transaction tx) {}

    public static int writeToLog(LogMgr lm, int txnum) {
        byte[] rec = new byte[2*Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, COMMIT);
        p.setInt(Integer.BYTES, txnum);
        return lm.append(rec);
    }
}
