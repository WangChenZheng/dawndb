package dawndb.file;

import java.io.File;
import java.io.IOException;


/**
 * @Author: WangChen
 * @Date: 2022/12/30 18:03
 * @Version: 1.0
 * @Description:
 */


public class FileMgrTest {

    public static void main(String[] args) throws IOException {
        // 创建数据库dbtest, 数据块大小400字节
        FileMgr fm = new FileMgr(new File("dbtest/file"), 400);

        // 创建文件stu.tbl, 该文件当前占用一个磁盘块
        BlockId blk = new BlockId("stu.tbl", 1);
        Page p1 = new Page(fm.blockSize());
        // 向文件stu.tbl的第88个字节开始写入"abcdefghijklmn"
        String s = "abcdefghijklmn";
        int pos1 = 88;
        p1.setString(pos1, s);

        // 在文件stu.tbl中, 在上次操作("abcdefghijklmn")后的位置写入345
        // 计算存储"abcdefghijklmn"实际所需空间
        int size = Page.maxLength(s.length());
        int pos2 = pos1 + size;
        p1.setInt(pos2, 345);

        // 将blk写回磁盘
        fm.write(blk, p1);

        // 将blk磁盘块读入p2内
        Page p2 = new Page(fm.blockSize());
        fm.read(blk, p2);
        // 输出pos1处的内容 pos2处的内容
        System.out.println("offset " + pos1 + " contains " + p2.getString(pos1));
        System.out.println("offset " + pos2 + " contains " + p2.getInt(pos2));
    }
}
