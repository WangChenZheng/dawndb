package dawndb.tx;

import dawndb.buffer.Buffer;
import dawndb.buffer.BufferMgr;
import dawndb.file.BlockId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 12:16
 * @Version: 1.0
 * @Description: 管理事务当前固定的缓冲区
 */


public class BufferList {

    private Map<BlockId, Buffer> buffers = new HashMap<>();
    private List<BlockId> pins = new ArrayList<>();
    private BufferMgr bm;

    public BufferList(BufferMgr bm) {
        this.bm = bm;
    }

    /**
     * 返回固定在指定块上的缓冲区。如果事务没有固定该块，则该方法返回null。
     */
    Buffer getBuffer(BlockId blk) {
        return buffers.get(blk);
    }

    /**
     * 固定块并在内部跟踪缓冲区。
     */
    void pin(BlockId blk) {
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    /**
     * 解除指定块的固定。
     */
    void unpin(BlockId blk) {
        Buffer buff = buffers.get(blk);
        bm.unpin(buff);
        pins.remove(blk);
        if (!pins.contains(blk)) {
            buffers.remove(blk);
        }
    }

    /**
     * 解除事务固定的所有缓冲区。
     */
    void unpinAll() {
        for (BlockId blk : pins) {
            Buffer buff = buffers.get(blk);
            bm.unpin(buff);
        }
        buffers.clear();
        pins.clear();
    }
}
