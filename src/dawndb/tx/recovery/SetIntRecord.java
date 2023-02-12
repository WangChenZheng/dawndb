package dawndb.tx.recovery;

import dawndb.file.BlockId;
import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 16:48
 * @Version: 1.0
 * @Description: SETINT类型日志记录
 */


public class SetIntRecord implements LogRecord{

    private int txnum, offset, val;
    private BlockId blk;

    public SetIntRecord(Page p) {
        int tpos = Integer.BYTES;
        txnum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new BlockId(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getInt(vpos);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        // 不会记录这个撤销操作
        tx.setInt(blk, offset, val, false);
        tx.unpin(blk);
    }

    @Override
    public String toString() {
        return "<SETINT " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, int val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.filename().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        byte[] rec = new byte[vpos + Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, SETINT);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.filename());
        p.setInt(bpos, blk.blknum());
        p.setInt(opos, offset);
        p.setInt(vpos, val);
        return lm.append(rec);
    }
}
