package dawndb.tx.recovery;

import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 12:45
 * @Version: 1.0
 * @Description: CHECKPOINT类型日志记录
 */


public class CheckpointRecord implements LogRecord{


    @Override
    public int op() {
        return CHECKPOINT;
    }

    /**
     * 检查点记录没有关联的事务, 故该方法返回一个虚假值-1
     */
    @Override
    public int txNumber() {
        return -1;
    }

    /**
     * 检查点记录无需撤销信息，故什么也不做
     * @param tx 正在执行撤销操作的事务
     */
    @Override
    public void undo(Transaction tx) {}

    @Override
    public String toString() {
        return "<CHECKPOINT>";
    }

    /**
     * 将检查点记录写入日志的静态方法。这个日志记录只包含CHECKPOINT操作符，没有其他内容。
     * <CHECKPOINT>
     * @return 最后一个日志值的LSN
     */
    public static int writeToLog(LogMgr lm) {
        byte[] rec = new byte[Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, CHECKPOINT);
        return lm.append(rec);
    }
}
