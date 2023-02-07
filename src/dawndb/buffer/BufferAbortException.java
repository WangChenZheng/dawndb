package dawndb.buffer;

/**
 * @Author: WangChen
 * @Date: 2023/1/10 21:00
 * @Version: 1.0
 * @Description:
 * 运行时异常，指示由于无法满足缓冲区请求而需要中止事务。
 */

@SuppressWarnings("serial")
public class BufferAbortException extends RuntimeException{
}
