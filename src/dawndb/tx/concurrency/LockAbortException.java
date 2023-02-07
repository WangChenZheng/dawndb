package dawndb.tx.concurrency;

/**
 * @Author: WangChen
 * @Date: 2023/2/7 19:37
 * @Version: 1.0
 * @Description: 运行时异常，指示由于无法获得锁而需要中止事务。
 */

@SuppressWarnings("serial")
public class LockAbortException extends RuntimeException {
}
