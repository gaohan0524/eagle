package com.alarm.eagle.cep.processor;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Abstract periodic polling discoverer.
 *
 * <p>Uses {@link java.util.Timer} for periodic rule checking.
 * Subclasses only need to implement two methods:
 * <ul>
 *   <li>{@link #arePatternProcessorsUpdated()} — check for updates</li>
 *   <li>{@link #getLatestPatternProcessors()} — get the latest rule list</li>
 * </ul>
 *
 * <p>业务含义：所有"周期性轮询规则源"发现器的公共骨架，用 {@code java.util.Timer}
 * 定时触发"检测变更 → 拉取最新规则 → 回调 manager"，子类只需实现两个数据访问方法。
 *
 * @param <T> event type
 */
public abstract class PeriodicPatternProcessorDiscoverer<T>
        implements PatternProcessorDiscoverer<T> {

    private final Long intervalMillis;
    private final Timer timer;

    /**
     * @param intervalMillis polling interval in milliseconds; first execution is immediate
     */
    protected PeriodicPatternProcessorDiscoverer(Long intervalMillis) {
        this.intervalMillis = intervalMillis;
        this.timer = new Timer();
    }

    // ======================== Subclass Implementation ========================

    /** ★ Check whether rules have been updated */
    public abstract boolean arePatternProcessorsUpdated();

    /** ★ Get the latest rule list */
    public abstract List<PatternProcessor<T>>
        getLatestPatternProcessors() throws Exception;

    // ======================== Scheduling Logic ========================

    /**
     * 启动周期轮询：首次立即执行，之后按固定间隔调度。
     *
     * <p>容错说明：TimerTask 内捕获所有异常，否则未捕获异常会终止整个 Timer 线程，
     * 导致规则轮询彻底停止（这是 java.util.Timer 的已知陷阱）。
     */
    @Override
    public void discoverPatternProcessorUpdates(
            PatternProcessorManager<T> manager) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (arePatternProcessorsUpdated()) {
                        List<PatternProcessor<T>> processors =
                                getLatestPatternProcessors();
                        manager.onPatternProcessorsUpdated(processors);
                    }
                } catch (Exception e) {
                    // ★ Critical: uncaught exceptions in TimerTask will kill the entire Timer thread
                    e.printStackTrace();
                }
            }
        }, 0, intervalMillis); // 0ms = execute immediately on first run, then at interval
    }

    @Override
    public void close() throws IOException {
        timer.cancel();
    }
}
