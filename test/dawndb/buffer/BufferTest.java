package dawndb.buffer;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.file.Page;
import dawndb.log.LogMgr;

import java.io.File;
import java.util.Arrays;

/**
 * @Author: WangChen
 * @Date: 2023/1/10 11:44
 * @Version: 1.0
 * @Description:
 */public class BufferTest {

    public static void main(String[] args) {
        FileMgr fm = new FileMgr(new File("dbtest"), 400);
        LogMgr lm = new LogMgr(fm, "db.log");
        Buffer buffer = new Buffer(fm, lm);
        Page p = buffer.contents();
        int n1 = p.getInt(80);
        p.setInt(80, n1+1);
        int n2 = p.getInt(80);
        System.out.println("磁盘中第80字节处的int类型数据值为："+n1+", 缓冲区中第80字节处的int类型数据值为"+n2);
        buffer.flush();
        System.out.println("将缓冲区数据写回磁盘");
        int n3 = p.getInt(80);
        BlockId blk = new BlockId("testfile", 1);
        buffer.assignToBlock(blk);
        p = buffer.contents();
        int n4 = p.getInt(80);
        System.out.println("磁盘中第80字节处的int类型数据值为："+n4+", 缓冲区中第80字节处的int类型数据值为"+n3);

    }
}



