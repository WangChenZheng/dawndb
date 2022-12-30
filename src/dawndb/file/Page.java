package dawndb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @Author: WangChen
 * @Date: 2022/12/29 20:53
 * @Version: 1.0
 * @Description:
 * Page对象保存磁盘块的内容。
 * blob字节数 blob内容
 */


public class Page {

    private ByteBuffer bb;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * 创建一个从操作系统 I/O 缓冲区获取内存的页
     * @param blockSize 块大小
     */
    public Page(int blockSize) {
        bb = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * 创建一个从 Java 数组获取内存的页面
     * @param b 字节数组
     */
    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
    }

    /**
     * 读取4个字节的数据
     * @param offset 起始位置
     * @return offset后4个字节的数据
     */
    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    /**
     * 写入4个字节的数据
     * @param offset 起始位置
     * @param n 被写入的数据
     */
    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }

    /**
     * 获取offset后的所有字节
     * @param offset 起始位置
     * @return
     */
    public byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        // 将缓冲区内容输出到b数组
        bb.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public static int maxLength(int strlen) {
        float bytesPreChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int)bytesPreChar);
    }

    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }
}
