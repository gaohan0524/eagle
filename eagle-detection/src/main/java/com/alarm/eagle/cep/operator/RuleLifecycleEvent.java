package com.alarm.eagle.cep.operator;

import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.Serializable;
import java.util.Objects;

/**
 * Rule lifecycle control event — triggered by external systems (e.g. REST API),
 * forwarded via Coordinator to TM side for execution.
 *
 * <p>Supported operations:
 * <ul>
 *   <li>{@code PAUSE} — Pause rule (NFA preserved, but no longer accepts new events)</li>
 *   <li>{@code RESUME} — Resume paused rule</li>
 *   <li>{@code DRAIN} — Graceful drain (no longer accepts new events, waiting for existing partial matches to timeout)</li>
 *   <li>{@code DELETE} — Force offline (immediately clean up NFA and all state)</li>
 * </ul>
 */
public class RuleLifecycleEvent implements OperatorEvent, Serializable {

    private static final long serialVersionUID = 1L;

    public enum Action {
        /** 暂停规则：NFA 与状态保留，但不再接收新事件，可随时恢复 */
        PAUSE,
        /** 恢复规则：从暂停/排空状态恢复接收事件 */
        RESUME,
        /** 优雅排空：不再接收新事件，等待已有部分匹配自然走完或超时 */
        DRAIN,
        /** 强制下线：立即关闭该规则所有版本的 NFA 并清理状态 */
        DELETE
    }

    private final Action action;
    private final String ruleId;

    public RuleLifecycleEvent(Action action, String ruleId) {
        this.action = Objects.requireNonNull(action, "action");
        this.ruleId = Objects.requireNonNull(ruleId, "ruleId");
    }

    public Action getAction() { return action; }
    public String getRuleId() { return ruleId; }

    @Override
    public String toString() {
        return "RuleLifecycleEvent{" + action + " " + ruleId + "}";
    }
}
