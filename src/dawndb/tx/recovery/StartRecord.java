package dawndb.tx.recovery;

import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 12:52
 * @Version: 1.0
 * @Description: START类型日志记录
 */


public class StartRecord implements LogRecord{

    private int txnum;

    /**
     * 通过从日志中读取另一个值(事务序列号)来创建日志记录。
     */
    public StartRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
    }

    @Override
    public int op() {
        return START;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    /**
     * 检查点记录无需撤销信息，故什么也不做
     * @param tx 正在执行撤销操作的事务
     */
    @Override
    public void undo(Transaction tx) {}

    /**
     * 将开始记录写入日志的静态方法。该日志记录包含START操作符，后面跟着事务id。
     * <START txnum>
     * @return 最后一个日志值的LSN
     */
    public static int writeToLog(LogMgr lm, int txnum) {
        byte[] rec = new byte[2*Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, START);
        p.setInt(Integer.BYTES, txnum);
        return lm.append(rec);
    }
}
