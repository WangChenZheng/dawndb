package dawndb.log;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.file.Page;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: WangChen
 * @Date: 2023/1/1 13:08
 * @Version: 1.0
 * @Description:
 * 倒序遍历日志文件记录
 */


public class LogIterator implements Iterator<byte[]> {

    private FileMgr fm;
    private BlockId blk;
    private Page p;
    private int currentpos;
    private int boundary;

    /**
     * 创建一个日志记录迭代器
     * 位于最后一个日志记录之后
     */
    public LogIterator(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        byte[] b = new byte[fm.blockSize()];
        p = new Page(b);
        moveToBlock(blk);
    }

    /**
     * 确定当前日志记录是否是日志文件中最早的记录
     */
    @Override
    public boolean hasNext() {
        return currentpos < fm.blockSize() || blk.blknum() > 0;
    }

    /**
     * 移动到块中的下一个日志记录。
     * 如果该块中没有更多的日志记录，则移动到前一个块并从那里返回日志记录。
     */
    @Override
    public byte[] next() {
        if (currentpos == fm.blockSize()) {
            blk = new BlockId(blk.filename(), blk.blknum()-1);
            moveToBlock(blk);
        }
        byte[] rec = p.getBytes(currentpos);
        currentpos += Integer.BYTES + rec.length;
        return rec;
    }

    /**
     * 移动到指定的日志块
     * 并将其定位在该块中的第一条记录
     */
    public void moveToBlock(BlockId blk) {
        fm.read(blk, p);
        boundary = p.getInt(0);
        currentpos = boundary;
    }
}
