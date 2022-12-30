package dawndb.file;

import java.util.Objects;

/**
 * @Author: WangChen
 * @Date: 2022/12/29 20:40
 * @Version: 1.0
 * @Description:
 * 通过文件名和逻辑块号标识特定块。
 */


public class BlockId {

    private String filename;
    private int blknum;

    public BlockId(String filename, int blknum) {

        this.filename = filename;
        this.blknum = blknum;
    }

    public String filename() {
        return filename;
    }

    public int blknum() {
        return blknum;
    }

    @Override
    public boolean equals(Object obj) {
        BlockId blk = (BlockId) obj;
        return filename.equals(blk.filename) && blknum == blk.blknum;
    }

    @Override
    public String toString() {
        return "[File: " + filename + ", Block: " + blknum + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
