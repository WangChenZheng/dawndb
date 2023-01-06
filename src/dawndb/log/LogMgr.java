package dawndb.log;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.file.Page;

import java.util.Iterator;


/**
 * @Author: WangChen
 * @Date: 2023/1/1 13:04
 * @Version: 1.0
 * @Description:
 * 内存页内容：最后写入记录的位置(boundary) + 日志内容(从右向左存储, 日志内容包括：字节数+内容)
 * fm: 负责读写日志块至磁盘
 * logfile: 日志文件
 * logpage: 日志文件页
 * currentblk: 当前应读写的日志文件块
 * latestLSN: 最新的日志序列号(LSN)
 * lastSavedLSN: 已写入磁盘的最新的日志序列号
 */


public class LogMgr {

    private FileMgr fm;
    private String logfile;
    private Page logpage;
    private BlockId currentblk;
    private int latestLSN;
    private int lastSavedLSN;

    public LogMgr(FileMgr fm, String logfile) {
        this.fm = fm;
        this.logfile = logfile;
        // 向内存中申请一块内存页
        byte[] b = new byte[fm.blockSize()];
        logpage = new Page(b);
        // 获取文件logfile所占的物理块个数
        int logsize = fm.length(logfile);
        if (logsize == 0) {
            // 若日志文件不存在
            currentblk = appendNewBlock();
        } else {
            // 日志文件存在，则读入前一个块(前一个块未满)
            currentblk = new BlockId(logfile, logsize-1);
            fm.read(currentblk, logpage);
        }
    }

    /**
     * 添加一条日志记录
     * 日志被从右向左存储，这样可以更好地被读
     * @param logrec
     * @return 最新日志记录的序列号LSN
     */
    public int append(byte[] logrec) {
        // 获取当前内存页已用空间
        int boundary = logpage.getInt(0);
        // 计算存储记录所需的字节数
        int recsize = logrec.length;
        int bytesneeded = recsize + Integer.BYTES;

        if (boundary - bytesneeded < Integer.BYTES) {
            // 无法存储记录，将当前页写回内存并使用下一块磁盘块
            flush();
            currentblk = appendNewBlock();
            boundary = logpage.getInt(0);
        }

        // 日志内存页剩余空间可以存储日志
        int recpos = boundary - bytesneeded;
        logpage.setBytes(recpos, logrec);
        logpage.setInt(0, recpos);
        latestLSN += 1;
        return latestLSN;
    }

    /**
     * 将lsn号之前的日志记录写回磁盘
     */
    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    /**
     * 访问日记记录的迭代器
     */
    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentblk);
    }

    /**
     * 申请新的磁盘块用于存储日志记录
     */
    private BlockId appendNewBlock() {
        BlockId blk = fm.append(logfile);
        logpage.setInt(0, fm.blockSize());
        fm.write(blk, logpage);
        return blk;
    }

    /**
     * 将内存页内容写入磁盘
     */
    private void flush() {
        fm.write(currentblk, logpage);
        lastSavedLSN = latestLSN;
    }

}
