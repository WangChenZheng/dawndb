package dawndb.tx.recovery;

import dawndb.file.BlockId;
import dawndb.file.Page;
import dawndb.log.LogMgr;
import dawndb.tx.Transaction;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 17:00
 * @Version: 1.0
 * @Description:
 */


public class SetStringRecord implements LogRecord {

    private int txnum, offset;
    private String val;
    private BlockId blk;

    public SetStringRecord(Page p) {
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
        val = p.getString(vpos);
    }

    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return txnum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false);
        tx.unpin(blk);
    }

    @Override
    public String toString() {
        return "<SETSTRING " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.filename().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());
        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txnum);
        p.setString(fpos, blk.filename());
        p.setInt(bpos, blk.blknum());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lm.append(rec);
    }
}
