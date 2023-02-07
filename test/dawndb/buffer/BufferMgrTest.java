package dawndb.buffer;

import dawndb.file.BlockId;
import dawndb.file.FileMgr;
import dawndb.log.LogMgr;

import java.io.File;

/**
 * @Author: WangChen
 * @Date: 2023/1/23 14:01
 * @Version: 1.0
 * @Description:
 */


public class BufferMgrTest {

    public static void main(String[] args) {
        FileMgr fm = new FileMgr(new File("dbtest"), 400);
        LogMgr lm = new LogMgr(fm, "db.log");
        BufferMgr bm = new BufferMgr(fm, lm, 3);

        Buffer[] buff = new Buffer[6];
        // 固定三个缓冲区块
        buff[0] = bm.pin(new BlockId("testfile", 0));
        buff[1] = bm.pin(new BlockId("testfile", 1));
        buff[2] = bm.pin(new BlockId("testfile", 2));
        // 取消固定缓冲区块1
        bm.unpin(buff[1]); buff[1] = null;
        // 重复固定缓冲区块0
        buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
        // 再次固定缓冲区块1
        buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
        System.out.println("可用的缓冲区个数为: " + bm.available());
        try {
            System.out.println("尝试固定第四个缓冲区块");
            // 将失败，因为没有多余的缓冲区块
            buff[5] = bm.pin(new BlockId("testfile", 3));
        } catch(BufferAbortException e) {
            System.out.println("Exception: No available buffers\n");
        }
        bm.unpin(buff[2]); buff[2] = null;
        // 成功
        buff[5] = bm.pin(new BlockId("testfile", 3));
        System.out.println("缓冲区分配如下:");
        for (int i=0; i<buff.length; i++) {
            Buffer b = buff[i];
            if (b != null)
                System.out.println("buff["+i+"] 被固定到块 " + b.block());
        }
    }
}
