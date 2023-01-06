package dawndb.log;

import dawndb.file.FileMgr;
import dawndb.file.Page;

import java.io.File;
import java.util.Iterator;

/**
 * @Author: WangChen
 * @Date: 2023/1/2 9:23
 * @Version: 1.0
 * @Description:
 */


public class LogMgrTest {

    private static LogMgr lm;

    public static void main(String[] args) {
        FileMgr fm = new FileMgr(new File("dbtest"), 400);
        lm = new LogMgr(fm, "/db.log");

        printLogRecords("The initial empty log file:");  // print an empty log file
        System.out.println("done");
        createRecords(1, 35);
        printLogRecords("The log file now has these records:");
        createRecords(36, 70);
        lm.flush(65);
        printLogRecords("The log file now has these records:");
    }

    /**
     * 输出当前日志块内容
     * 初始时日志块内容为磁盘中最后一个日志块的内容
     */
    private static void printLogRecords(String msg) {
        System.out.println(msg);
        Iterator<byte[]> iter = lm.iterator();
        // 日志块非空
        while (iter.hasNext()) {
            byte[] rec = iter.next();
            Page p = new Page(rec);
            // 日志内容
            String s = p.getString(0);
            int npos = Page.maxLength(s.length());
            int val = p.getInt(npos);
            System.out.println("[" + s + ", " + val + "]");
        }
        System.out.println();
    }

    private static void createRecords(int start, int end) {
        System.out.print("Creating records: ");
        for (int i=start; i<=end; i++) {
            byte[] rec = createLogRecord("record"+i, i);
            int lsn = lm.append(rec);
            System.out.print(lsn + " ");
        }
        System.out.println();
    }

    private static byte[] createLogRecord(String s, int n) {
        int spos = 0;
        int npos = spos + Page.maxLength(s.length());
        byte[] b = new byte[npos + Integer.BYTES];
        Page p = new Page(b);
        p.setString(spos, s);
        p.setInt(npos, n);
        return b;
    }
}
