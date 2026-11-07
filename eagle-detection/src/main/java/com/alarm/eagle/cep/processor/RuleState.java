package com.alarm.eagle.cep.processor;

/**
 * Rule lifecycle state.
 *
 * <p>规则生命周期状态枚举。注意：当前代码中该枚举未被引用，
 * 实际的暂停/排空控制走的是 {@code DynamicCepOperator} 内部的
 * {@code pausedRuleIds}/{@code drainingRuleIds} 集合，而非此枚举。
 */
public enum RuleState {
    /** 活跃：正在正常处理事件 */
    ACTIVE,
    /** 已暂停：NFA 与状态保留，但不再接收新事件 */
    PAUSED,
    /** 排空中：不再接收新事件，等待已有部分匹配自然超时 */
    DRAINING,
    /** 已停止 */
    STOPPED,
    /** 错误状态 */
    ERROR
}
