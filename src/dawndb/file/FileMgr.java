package dawndb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: WangChen
 * @Date: 2022/12/30 12:00
 * @Version: 1.0
 * @Description:
 * 完成页面与磁盘块的交互，包括读写页面到磁盘块。
 *
 * dbDirectory数据库的文件指针
 * blocksize逻辑物理块的大小
 * isNew数据库是否存在
 * openFiles打开文件列表
 */


public class FileMgr {

    private File dbDirectory;
    private int blocksize;
    private boolean isNew;
    private Map<String, RandomAccessFile> openFiles = new HashMap<>();

    public FileMgr(File dbDirectory, int blocksize) {
        this.dbDirectory = dbDirectory;
        this.blocksize = blocksize;
        isNew = !dbDirectory.exists();

        // 若数据库不存在，则创建数据库
        if (isNew) {
            dbDirectory.mkdirs();
        }

        // 删除所有临时表
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.filename());
            f.seek(blk.blknum() * blocksize);
            f.getChannel().read(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("无法读取块 " + blk);
        }
    }

    public synchronized void write(BlockId blk, Page p) {
        try {
            RandomAccessFile f = getFile(blk.filename());
            f.seek(blk.blknum() * blocksize);
            f.getChannel().write(p.contents());
        } catch (IOException e) {
            throw new RuntimeException("无法写入块 " + blk);
        }
    }

    public synchronized BlockId append(String filename) {
        int newBlkNum = length(filename);
        BlockId blk = new BlockId(filename, newBlkNum);
        byte[] b = new byte[blocksize];

        try {
            RandomAccessFile f = getFile(blk.filename());
            f.seek(blk.blknum() * blocksize);
            f.write(b);
        } catch (IOException e) {
            throw new RuntimeException("无法添加块" + blk);
        }

        return blk;
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blocksize;
    }

    public int length(String filename) {
        try {
            RandomAccessFile f = getFile(filename);
            return (int)(f.length() / blocksize);
        }
        catch (IOException e) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile f = openFiles.get(filename);
        if (f == null) {
            File dbTable = new File(dbDirectory, filename);
            f = new RandomAccessFile(dbTable, "rws");
            openFiles.put(filename, f);
        }
        return f;
    }

}
