package dawndb.buffer;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.file.Page;
import dawndb.log.LogMgr;

/**
 * @Author: WangChen
 * @Date: 2023/1/10 11:36
 * @Version: 1.0
 * @Description:
 * 缓冲区
 * fm: 文件管理器
 * lm: 日志管理器
 * contents: 缓冲区内容
 * blk: 分配给缓冲区的磁盘块
 * pins: 当前固定块的个数
 * txnum: 事务序列号
 * lsn: 日志序列号
 */


public class Buffer {

    private FileMgr fm;
    private LogMgr lm;
    private Page contents;
    private BlockId blk = null;
    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    /**
     * 初始化缓冲区
     */
    public Buffer(FileMgr fm, LogMgr lm) {
        this.fm = fm;
        this.lm = lm;
        contents = new Page(fm.blockSize());
    }

    /**
     * 缓冲区所对应的页面
     */
    public Page contents() {
        return contents;
    }

    /**
     * 缓冲区对应的块
     */
    public BlockId block() {
        return blk;
    }

    /**
     * 更新事务序列号及日志序列号
     */
    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    /**
     * 当前缓冲区是否被引用
     */
    public boolean isPinned() {
        return pins > 0;
    }

    /**
     * 返回当前事务序列号
     */
    public int modifyingTx() {
        return txnum;
    }

    /**
     * 读取指定块的内容进缓冲区，若缓冲区不为空则先将缓冲区内容写回磁盘
     */
    void assignToBlock(BlockId blk) {
        flush();
        this.blk = blk;
        fm.read(this.blk, contents);
        pins = 0;
    }

    /**
     * 将缓冲区内容写回磁盘
     */
    void flush() {
        if (txnum >= 0) {
            lm.flush(lsn);
            fm.write(blk, contents);
            txnum = -1;
        }
    }

    /**
     * 增加缓冲区块的引用数
     */
    void pin() {
        pins++;
    }

    /**
     * 减少缓冲区块的引用数
     */
    void unpin() {
        pins--;
    }
}
