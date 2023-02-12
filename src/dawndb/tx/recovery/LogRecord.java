package dawndb.tx.recovery;

import dawndb.file.Page;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 12:40
 * @Version: 1.0
 * @Description: 由每种日志记录类型实现的接口。
 */

public interface LogRecord {

     static final int CHECKPOINT = 0;
    static final int START = 1;
    static final int COMMIT = 2;
    static final int ROLLBACK = 3;
    static final int SETINT = 4;
    static final int SETSTRING = 5;

    /**
     * 返回日志记录类型
     */
    int op();

    /**
     * 返回与日志记录一起存储的事务id
     */
    int txNumber();

    /**
     * 撤消此日志记录编码的操作。只有SETINT和SETSTRING两种日志记录类型是这个方法所处理的。
     * @param tx 正在执行撤销操作的事务
     */
    void undo(Transaction tx);

    static LogRecord createLogRecord(byte[] bytes) {
        Page p = new Page(bytes);
        switch (p.getInt(0)) {
            case CHECKPOINT:
                return new CheckpointRecord();
            case START:
                return new StartRecord(p);
            case COMMIT:
                return new CommitRecord(p);
            case ROLLBACK:
                return new RollbackRecord(p);
            case SETINT:
                return new SetIntRecord(p);
            case SETSTRING:
                return new SetStringRecord(p);
            default:
                return null;
        }
    }
}
