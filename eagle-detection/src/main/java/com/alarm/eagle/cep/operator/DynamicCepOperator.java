package com.alarm.eagle.cep.operator;

import com.alarm.eagle.cep.processor.PatternProcessor;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * ★★★ Dynamic CEP core operator (Phase 1 multi-rule parallel edition).
 *
 * <p>Differences from the MVP single-rule version:
 * <ul>
 *   <li>Single NFA → {@code Map<PatternProcessor, NFA>} multi-NFA parallel</li>
 *   <li>Single state set → per-rule independent state (named with {@code {name}-{id}-{version}} suffix)</li>
 *   <li>Per-rule independent TimerService</li>
 *   <li>Per-rule independent Metrics Counter</li>
 *   <li>The same event is evaluated in parallel by all active NFAs</li>
 * </ul>
 *
 * <p><b>Current limitation</b>: SharedBuffer is shared across rules (no actual conflict when different rules have different state names).
 *
 * <p><b>本类的作用</b>
 * <pre>
 *   它是 Dynamic CEP 引擎的"执行引擎"，运行在 TaskManager 上，
 *   是一个同时扮演三种角色的 Flink 底层算子：
 *
 *     1. OneInputStreamOperator —— 处理输入事件流       (processElement)
 *     2. Triggerable            —— 响应事件/处理时间定时器 (onEventTime / onProcessingTime)
 *     3. OperatorEventHandler   —— 接收 JM 下发的规则变更  (handleOperatorEvent)
 *
 *   一句话概括：它把"一条静态的 Flink CEP 作业"改造成了
 *   "一个能同时跑 N 条规则、且规则可在运行时增删改"的引擎。
 * </pre>
 *
 * <p><b>三大核心机制</b>
 * <ul>
 *   <li><b>多规则并行</b>：内部维护 {@code Map<RuleKey, RuleContext>}，
 *       一条规则 = 一个 NFA + 一套独立状态 + 一套独立定时器 + 一套独立指标；
 *       同一条事件会被所有活跃规则并行评估</li>
 *   <li><b>状态版本隔离</b>：所有状态名都带 {@code {id}-{version}} 后缀，
 *       因此规则升版后新 NFA 从零开始，旧版本状态互不干扰且可被回收</li>
 *   <li><b>规则热插拔</b>：通过 OperatorEvent 接收规则变更，
 *       用"构建新 Map → 原子替换引用"的方式切换规则集，全程不重启作业</li>
 * </ul>
 *
 * @param <IN>  Input event type
 * @param <KEY> Key type
 * @param <OUT> Output type
 */
public class DynamicCepOperator<IN, KEY, OUT>
        extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>,
                   Triggerable<KEY, VoidNamespace>,
                   OperatorEventHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicCepOperator.class);
    private static final long serialVersionUID = 2L;

    // ======================== State Name Prefix Constants ========================

    /**
     * 状态名前缀。
     *
     * <p>实际使用时的完整状态名形如 {@code "nfaState-rule_login_bruteforce-1"}，
     * 即 "{前缀}-{规则id}-{版本号}"。
     *
     * <p>★ 这是"不同规则、不同版本之间状态物理隔离"的关键所在：
     * 规则升级版本后会读写一个全新的状态名，天然读不到旧版本的脏数据，
     * 也就不需要在升级时做复杂的状态迁移。
     */
    private static final String NFA_STATE_PREFIX = "nfaState";
    private static final String EVENT_QUEUE_PREFIX = "eventQueue";
    private static final String TIMER_SERVICE_PREFIX = "watermark-callbacks";

    // ======================== Constructor Parameters ========================

    private final boolean isProcessingTime;
    private final TypeSerializer<IN> inputSerializer;

    // ======================== ★ Multi-Rule Data Structures (Core Changes) ========================

    /**
     * ★ Rule → NFA mapping. Each rule (id + version) corresponds to one NFA instance.
     * Replace this Map entirely on rule update.
     *
     * <p>本算子的"中央注册表"：规则唯一标识 (id+version) → 该规则的运行时上下文。
     * 热更新时采用"新建 Map → 整体替换引用"的方式切换（见 {@link #handleUpdatePatternProcessorEvent}），
     * 而不是在旧 Map 上原地增删，这样可以避免遍历过程中的并发修改问题。
     */
    private transient Map<RuleKey, RuleContext<IN, OUT>> ruleContexts;

    /**
     * ★ SharedBuffer — shared by all rules (current limitation).
     * Can be upgraded later to per-rule independent SharedBuffer (requires modifying Flink SharedBuffer source).
     *
     * <p>部分匹配（partial match）的共享缓冲区，存放各 NFA 的中间匹配结果。
     *
     * <p>★ 已知局限：所有规则共用同一个 SharedBuffer，并没有做到 per-rule 隔离。
     * 之所以目前不冲突，是因为各规则在其中的节点 key 由各自的状态名区分。
     * 若要做真正的隔离，需要修改 Flink SharedBuffer 源码。
     */
    private transient SharedBuffer<IN> sharedPartialMatches;

    // ======================== Deferred State Cleanup (executed when key context is available) ========================

    /**
     * Marks which old rules need state cleanup (key = RuleKey).
     *
     * <p><b>Known limitation</b>: ValueState is per-key, processElement can only clean up the current key
     * state. Old state for other keys becomes orphaned, relying on Flink State TTL for async reclamation.
     * Can be upgraded later to use RawKeyedStateBackend for direct batch cleanup.
     *
     * <p>"待清理旧规则状态"队列。
     *
     * <p>★ 为什么要延迟清理？因为 {@code handleOperatorEvent} 是由 RPC 线程调用的，
     * 此时没有 key 上下文，无法调用 {@code ValueState.clear()}；
     * 所以只能先登记，等到下一个 {@code processElement}（此时已确定当前 key）再统一清理。
     *
     * <p>★ 该方案的副作用：只能清掉"当前 key"下面的旧状态，
     * 其余 key 下的旧状态会变成孤儿，只能依赖 Flink State TTL 异步回收。
     */
    private transient final List<RuleKey> pendingStateClear = new ArrayList<>();

    /**
     * ★ Paused rule IDs (skipped in processElement)
     *
     * <p>已暂停的规则 id 集合。
     * 语义是"软暂停"：NFA 与状态原样保留，只是不再接收新事件，可随时 RESUME 恢复。
     */
    private transient final java.util.Set<String> pausedRuleIds = new java.util.HashSet<>();

    /**
     * ★ Draining rule IDs (no longer accept new events, only process existing partial matches)
     *
     * <p>排空中的规则 id 集合。
     * 语义是"优雅下线"：不再接收新事件，但让已有的部分匹配自然走完或超时。
     */
    private transient final java.util.Set<String> drainingRuleIds = new java.util.HashSet<>();

    // ======================== Runtime Components ========================

    private transient TimestampedCollector<OUT> collector;
    private transient ContextFunctionImpl context;
    private transient StateInitializationContext initContext;

    public DynamicCepOperator(
            TypeSerializer<IN> inputSerializer,
            boolean isProcessingTime) {
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
        this.isProcessingTime = isProcessingTime;
    }

    // ======================== Lifecycle ========================

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            org.apache.flink.streaming.api.graph.StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
    }

    /**
     * 算子状态初始化。
     *
     * <p>注意：此处只创建 SharedBuffer 和一个空的 {@code ruleContexts} Map，
     * ★并不加载任何规则★。规则是后续通过 OperatorEvent 由 JobManager 推送过来的。
     *
     * <p>这正是"动态"的第一层含义——算子启动时对规则一无所知，
     * 因此它可以先于规则启动，也能在规则被全部清空时空转等待而不报错。
     */
    @Override
    public void initializeState(StateInitializationContext context)
            throws Exception {
        LOG.info("[CEP] initializeState() started...");
        super.initializeState(context);
        LOG.info("[CEP] super.initializeState() completed");
        this.initContext = context;

        // ★ SharedBuffer: shared by all rules (current limitation)
        this.sharedPartialMatches = new SharedBuffer<>(
                context.getKeyedStateStore(),
                inputSerializer,
                SharedBufferCacheConfig.of(
                        getOperatorConfig().getConfiguration()));

        this.ruleContexts = new HashMap<>();
        LOG.info("[CEP] initializeState() completed");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        LOG.info("[CEP] open() started...");
        super.open();
        LOG.info("[CEP] super.open() completed");

        collector = new TimestampedCollector<>(output);
        context = new ContextFunctionImpl();

        LOG.info("[CEP] open() completed (rules will be dynamically loaded upon receiving OperatorEvent)");
    }

    /**
     * 算子关闭：逐个关闭所有规则的 NFA 与用户函数，并释放 SharedBuffer 的缓存统计定时器。
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (ruleContexts != null) {
            for (RuleContext<IN, OUT> rc : ruleContexts.values()) {
                closeRuleContext(rc);
            }
            ruleContexts.clear();
        }
        if (sharedPartialMatches != null) {
            sharedPartialMatches.releaseCacheStatisticsTimer();
        }
    }

    // ======================== Event Processing (core — broadcast event to all rules)====================

    /**
     * 事件处理主流程（本类最核心的方法之一），共三步：
     * <ol>
     *   <li><b>补做清理</b>：执行上一次热更新遗留的旧规则状态清理。
     *       必须放在这里做，因为只有此刻才具备 key 上下文（见 pendingStateClear 的说明）</li>
     *   <li><b>无规则保护</b>：若当前一条规则都没有，直接丢弃事件并等待规则下发</li>
     *   <li><b>广播评估</b>：把同一条事件★依次交给所有规则的 NFA★并行评估，
     *       各规则互不干扰；单条规则抛异常会被 catch 住，不影响其他规则</li>
     * </ol>
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // ★ Deferred cleanup of old rule state (with key context)
        if (!pendingStateClear.isEmpty()) {
            for (RuleKey key : pendingStateClear) {
                RuleContext<IN, OUT> oldRc = ruleContexts.get(key);
                if (oldRc != null) {
                    oldRc.computationStates.clear();
                    oldRc.elementQueueState.clear();
                }
            }
            pendingStateClear.clear();
        }

        if (ruleContexts.isEmpty()) {
            LOG.info("[CEP] ⚠ Event arrived but no active rules, dropping and waiting...");
            return;
        }

        // ★ Each event is evaluated by all rules in parallel
        for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                ruleContexts.entrySet()) {
            RuleKey ruleKey = entry.getKey();
            RuleContext<IN, OUT> rc = entry.getValue();

            try {
                processElementForRule(element, ruleKey, rc);
            } catch (Exception e) {
                LOG.error("Error processing event for rule {}-v{}: {}",
                        ruleKey.id, ruleKey.version, e.getMessage(), e);
            }
        }
    }

    /**
     * Processing logic for a single event per rule
     *
     * <p>单条规则处理单条事件的分发逻辑。方法开头先做暂停/排空判定，
     * 然后按时间语义走两条完全不同的路：
     * <ul>
     *   <li><b>ProcessingTime</b> —— 立即用当前处理时间推进 NFA 并匹配，无需缓冲</li>
     *   <li><b>EventTime</b> —— 先判断是否迟到：
     *       {@code timestamp > 当前 watermark} 才算有效，写入该规则自己的缓冲队列等定时器触发；
     *       否则（已落后于 watermark）计入 {@code numLateRecordsDropped} 并丢弃</li>
     * </ul>
     */
    private void processElementForRule(
            StreamRecord<IN> element,
            RuleKey ruleKey,
            RuleContext<IN, OUT> rc) throws Exception {

        // ★ Phase 5: skip paused or draining rules
        if (shouldSkipRule(ruleKey.id)) {
            return;
        }

        if (isProcessingTime) {
            NFAState nfaState = getNFAState(rc);
            long timestamp = getProcessingTimeService()
                    .getCurrentProcessingTime();
            advanceTime(rc, nfaState, timestamp);
            processEvent(rc, nfaState, element.getValue(), timestamp);
            updateNFA(rc, nfaState);
        } else {
            long timestamp = element.getTimestamp();
            IN value = element.getValue();

            if (timestamp > rc.timerService.currentWatermark()) {
                bufferEvent(rc, value, timestamp);
            } else {
                rc.numLateRecordsDropped.inc();
            }
        }
    }

    // ======================== Timer Trigger (Iterate all rules) ========================

    /**
     * 事件时间定时器回调（由 watermark 推进触发）。
     *
     * <p>★ 值得注意：这里的实现是"遍历所有规则"逐个回放各自的缓冲队列，
     * 而不是只处理触发定时器的那一条规则。
     * 因为每条规则有独立的 InternalTimerService，任一规则的定时器触发都会进入本方法，
     * 方法内部再对全部规则做一次补回放，确保没有规则被遗漏。
     * 代价是存在一定的重复扫描（各规则内部会自行比对水位，未到时间则直接跳过）。
     */
    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer)
            throws Exception {
        for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                ruleContexts.entrySet()) {
            RuleContext<IN, OUT> rc = entry.getValue();
            processTimedEvents(rc, false);
        }
    }

    /**
     * 处理时间定时器回调，语义与 {@link #onEventTime} 相同，
     * 唯一区别是回放时不比较 watermark（处理时间语义下无需等待水位）。
     */
    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer)
            throws Exception {
        for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                ruleContexts.entrySet()) {
            RuleContext<IN, OUT> rc = entry.getValue();
            processTimedEvents(rc, true);
        }
    }

    /**
     * 定时器触发时，对"单条规则"回放其缓冲队列中的事件。
     *
     * <p>核心动作：把该规则 {@code elementQueueState} 中"时间已到"的事件
     * （EventTime 下指 {@code 事件时间 ≤ 当前 watermark}）按时间戳升序取出，
     * 先 advanceTime 推进 NFA 时间线，再逐条 processEvent 喂给 NFA，最后移除该时间戳。
     *
     * <p>★ 这一步保证了"同一个 key 内的事件严格按时间顺序被 CEP 处理"，
     * 即使它们到达算子的物理顺序是乱的——这正是 EventTime 语义下结果正确的前提。
     */
    private void processTimedEvents(RuleContext<IN, OUT> rc, boolean isProcTime)
            throws Exception {
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps(rc);
        NFAState nfaState = getNFAState(rc);

        while (!sortedTimestamps.isEmpty()
                && (isProcTime || sortedTimestamps.peek()
                        <= rc.timerService.currentWatermark())) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(rc, nfaState, timestamp);
            List<IN> elements = rc.elementQueueState.get(timestamp);
            if (elements != null) {
                for (IN event : elements) {
                    processEvent(rc, nfaState, event, timestamp);
                }
            }
            rc.elementQueueState.remove(timestamp);
        }

        advanceTime(rc, nfaState,
                isProcTime ? rc.timerService.currentProcessingTime()
                        : rc.timerService.currentWatermark());
        updateNFA(rc, nfaState);
    }

    // ======================== Core Matching Logic (Consistent with MVP) ========================

    /**
     * 单条规则的单条事件匹配入口，直接调用 Flink NFA 原生 API {@code nfa.process(...)}。
     *
     * <p>在此之外多做了一件事：如果本规则设置了 {@code within} 窗口
     * （{@code nfa.getWindowTime() > 0}），且本次产生了新的部分匹配起点，
     * 就注册一个"事件时间 + 窗口长度"的定时器。
     * ★ 这正是超时部分匹配后续能被清理、并能回调
     * {@code TimedOutPartialMatchHandler} 的前提条件。
     */
    private void processEvent(
            RuleContext<IN, OUT> rc,
            NFAState nfaState, IN event, long timestamp) throws Exception {
        AfterMatchSkipStrategy skipStrategy = getAfterMatchSkipStrategy(rc);

        try (SharedBufferAccessor<IN> accessor =
                     sharedPartialMatches.getAccessor()) {
            Collection<Map<String, List<IN>>> patterns =
                    rc.nfa.process(accessor, nfaState, event, timestamp,
                            skipStrategy, rc.cepTimerService);

            if (rc.nfa.getWindowTime() > 0
                    && nfaState.isNewStartPartialMatch()) {
                registerTimer(rc,
                        timestamp + rc.nfa.getWindowTime());
            }
            processMatchedSequences(rc, patterns, timestamp);
        }
    }

    /**
     * 把 NFA 的时间线推进到指定时间戳，返回两部分结果：
     * <ul>
     *   <li>{@code f0} —— 因窗口内完成而命中的完整匹配 → 交给 processMatchedSequences</li>
     *   <li>{@code f1} —— 因超出 within 窗口而超时的部分匹配 → 交给 processTimedOutSequences
     *       （只有用户函数实现了 {@code TimedOutPartialMatchHandler} 时才会回调）</li>
     * </ul>
     */
    private void advanceTime(
            RuleContext<IN, OUT> rc,
            NFAState nfaState, long timestamp) throws Exception {
        AfterMatchSkipStrategy skipStrategy = getAfterMatchSkipStrategy(rc);

        try (SharedBufferAccessor<IN> accessor =
                     sharedPartialMatches.getAccessor()) {
            Tuple2<
                    Collection<Map<String, List<IN>>>,
                    Collection<Tuple2<Map<String, List<IN>>, Long>>
                    > result = rc.nfa.advanceTime(
                            accessor, nfaState, timestamp, skipStrategy);

            if (!result.f0.isEmpty()) {
                processMatchedSequences(rc, result.f0, timestamp);
            }
            if (!result.f1.isEmpty()) {
                processTimedOutSequences(rc, result.f1);
            }
        }
    }

    /**
     * 命中处理：逐条调用用户注册的 {@code PatternProcessFunction.processMatch()}，
     * 并通过 {@code TimestampedCollector} 输出到下游。
     *
     * <p>★ 关键点：
     * <ul>
     *   <li>{@code setTimestamp(timestamp)} 会设置输出记录的时间戳，
     *       保证 EventTime 下告警记录携带正确的时间</li>
     *   <li>每次命中都会给<b>该规则自己</b>的 {@code patternMatchedTimes} 计数器 +1，
     *       因此可以在 Flink UI 上按规则分别观察命中量</li>
     * </ul>
     */
    @SuppressWarnings("unchecked")
    private void processMatchedSequences(
            RuleContext<IN, OUT> rc,
            Iterable<Map<String, List<IN>>> matchingSequences,
            long timestamp) throws Exception {
        setTimestamp(timestamp);
        for (Map<String, List<IN>> match : matchingSequences) {
            rc.patternMatchedTimes.inc();
            rc.userFunction.processMatch(
                    match, context, (Collector<OUT>) collector);
        }
    }

    /**
     * 超时部分匹配处理。
     *
     * <p>★ 只有当用户的 {@code PatternProcessFunction} 同时实现了
     * {@code TimedOutPartialMatchHandler} 接口时才会生效，否则静默丢弃。
     * 典型用途：统计"有多少笔交易走到了一半但最终没构成欺诈"，用于半命中分析。
     */
    @SuppressWarnings("unchecked")
    private void processTimedOutSequences(
            RuleContext<IN, OUT> rc,
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut)
            throws Exception {
        if (rc.userFunction instanceof TimedOutPartialMatchHandler) {
            TimedOutPartialMatchHandler<IN> handler =
                    (TimedOutPartialMatchHandler<IN>) rc.userFunction;
            for (Tuple2<Map<String, List<IN>>, Long> match : timedOut) {
                setTimestamp(match.f1);
                handler.processTimedOutMatch(match.f0, context);
            }
        }
    }

    // ======================== State Read/Write(Per Rule) ========================

    /**
     * 读取指定规则的 NFA 计算状态；若为空（该 key 首次见到这条规则），
     * 则用 {@code nfa.createInitialNFAState()} 造一个初始状态。
     */
    private NFAState getNFAState(RuleContext<IN, OUT> rc) throws IOException {
        NFAState state = rc.computationStates.value();
        return state != null ? state : rc.nfa.createInitialNFAState();
    }

    /**
     * 把变更后的 NFA 计算状态写回该规则的 ValueState。
     * ★ 只在 {@code isStateChanged()} 为真时才写，避免每条事件都产生一次状态序列化开销。
     */
    private void updateNFA(RuleContext<IN, OUT> rc, NFAState state)
            throws IOException {
        if (state.isStateChanged()) {
            state.resetStateChanged();
            state.resetNewStartPartialMatch();
            rc.computationStates.update(state);
        }
    }

    // ======================== Event Buffering(Per Rule) ========================

    /**
     * 为指定规则注册定时器。
     *
     * <p>★ 注意这里用的是 {@code rc.timerService}，即<b>每条规则自己的</b>
     * InternalTimerService，而不是算子全局的定时器服务。
     * 这正是"规则之间定时器互不干扰、各自按自己的节奏推进"的实现基础。
     */
    private void registerTimer(RuleContext<IN, OUT> rc, long timestamp) {
        if (isProcessingTime) {
            rc.timerService.registerProcessingTimeTimer(
                    VoidNamespace.INSTANCE, timestamp + 1);
        } else {
            rc.timerService.registerEventTimeTimer(
                    VoidNamespace.INSTANCE, timestamp);
        }
    }

    /**
     * EventTime 语义下的事件缓冲。
     *
     * <p>事件不会立刻匹配，而是先按时间戳存入<b>该规则自己的</b> {@code elementQueueState}，
     * 并对该时间戳注册一次定时器；等到 watermark 推进（{@code onEventTime} 触发）后再按序回放。
     *
     * <p>★ 每条规则有独立的缓冲队列，因此不同规则的回放进度互不影响——
     * 例如规则 A 配 5 分钟窗口、规则 B 配 30 分钟窗口，两者的缓冲与清理完全解耦。
     */
    private void bufferEvent(RuleContext<IN, OUT> rc,
                             IN event, long currentTime) throws Exception {
        List<IN> elements = rc.elementQueueState.get(currentTime);
        if (elements == null) {
            elements = new ArrayList<>();
            registerTimer(rc, currentTime);
        }
        elements.add(event);
        rc.elementQueueState.put(currentTime, elements);
    }

    /**
     * 把该规则缓冲队列中出现过的所有时间戳取出，放进小顶堆按升序排列。
     * 这样 {@code processTimedEvents} 就能严格按时间先后回放事件。
     */
    private PriorityQueue<Long> getSortedTimestamps(
            RuleContext<IN, OUT> rc) throws Exception {
        PriorityQueue<Long> sorted = new PriorityQueue<>();
        for (Long ts : rc.elementQueueState.keys()) {
            sorted.offer(ts);
        }
        return sorted;
    }

    private AfterMatchSkipStrategy getAfterMatchSkipStrategy(
            RuleContext<IN, OUT> rc) {
        if (rc.processor != null) {
            try {
                return Optional.ofNullable(
                        rc.processor.getPattern(getUserCodeClassloader())
                                .getAfterMatchSkipStrategy())
                        .orElse(AfterMatchSkipStrategy.noSkip());
            } catch (Exception e) {
                LOG.warn("Failed to get skip strategy, using noSkip.", e);
            }
        }
        return AfterMatchSkipStrategy.noSkip();
    }

    private void setTimestamp(long timestamp) {
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }
        context.setTimestamp(timestamp);
    }

    // ======================== OperatorEvent Handling (Rule Hot Swap)====================

    /**
     * ★热更新总入口★：接收 JobManager 通过 RPC 下发的规则事件，共三类：
     * <ol>
     *   <li>{@link UpdatePatternProcessorEvent} —— <b>全量规则集</b>，
     *       用于 TM 就绪 / 故障恢复后对齐基线</li>
     *   <li>{@link PatternProcessorChangeEvent} —— <b>增量变更</b>（新增 / 删除 / 升级），
     *       日常热更新走这条，开销最小</li>
     *   <li>{@link RuleLifecycleEvent} —— <b>生命周期控制</b>（暂停 / 恢复 / 排空 / 删除）</li>
     * </ol>
     *
     * <p>★ 重要约束：本方法由 RPC 线程调用，<b>没有 key 上下文</b>，
     * 因此不能直接清理 keyed state，只能在方法内登记到 {@code pendingStateClear} 延后处理。
     */
    @Override
    @SuppressWarnings("unchecked")
    public void handleOperatorEvent(OperatorEvent event) {
        try {
            if (event instanceof UpdatePatternProcessorEvent) {
                // ★ TM restart recovery: full replacement
                LOG.info("[CEP] Received full rule set event, total: "
                        + ((UpdatePatternProcessorEvent<IN>) event)
                            .getProcessors().size()
                        + "");
                handleFullUpdate((UpdatePatternProcessorEvent<IN>) event);
            } else if (event instanceof PatternProcessorChangeEvent) {
                // ★ incremental change: added / removed / updated
                PatternProcessorChangeEvent<IN> change =
                        (PatternProcessorChangeEvent<IN>) event;
                LOG.info("[CEP] Received incremental change event: +"
                        + change.getAdded().size() + " added, ~"
                        + change.getRemoved().size() + " removed, ↑"
                        + change.getUpdated().size() + " updated");
                handleIncrementalChange(change);
            } else if (event instanceof RuleLifecycleEvent) {
                // ★ Rule lifecycle control
                handleLifecycleEvent((RuleLifecycleEvent) event);
            } else {
                LOG.warn("Received unexpected operator event: {}", event);
            }
            LOG.info("[CEP] Event handling completed, current: " + ruleContexts.size()
                    + " rules active");
        } catch (Exception e) {
            LOG.error("[CEP] ★★★ handleOperatorEvent Exception!★★★");
            e.printStackTrace(System.err);
            throw new RuntimeException(
                    "Failed to update pattern processors.", e);
        }
    }

    /**
     * ★ Handle incremental change event (add/remove/update).
     * Only affects changed rules; other rules remain untouched.
     *
     * <p>增量热更新。相比全量替换，它只重建"发生变化"的那几条规则，
     * 其余规则的 NFA 与状态完全不动——这是日常规则变更时的最优路径。
     *
     * <p>★ 处理顺序固定为 <b>先删 → 再改 → 最后增</b>，
     * 这样可以保证同一条规则在同一轮变更里不会被重复处理。
     */
    @SuppressWarnings("unchecked")
    private void handleIncrementalChange(
            PatternProcessorChangeEvent<IN> change) throws Exception {

        // ① Handle removed (deletion)
        for (PatternProcessorChangeEvent.RuleIdentifier removed :
                change.getRemoved()) {
            RuleKey key = new RuleKey(removed.getId(), removed.getVersion());
            RuleContext<IN, OUT> oldRc = ruleContexts.get(key);
            if (oldRc != null) {
                pendingStateClear.add(key);
                closeRuleContext(oldRc);
                ruleContexts.remove(key);
                LOG.info("[CEP] Rule offline: " + key);
            }
        }

        // ② Handle updated (version upgrade)
        for (PatternProcessor<IN> processor : change.getUpdated()) {
            // Close old version
            RuleKey oldKey = findExistingRuleById(processor.getId());
            if (oldKey != null) {
                RuleContext<IN, OUT> oldRc = ruleContexts.get(oldKey);
                pendingStateClear.add(oldKey);
                closeRuleContext(oldRc);
                ruleContexts.remove(oldKey);
            }

            // Create new version
            RuleKey newKey = new RuleKey(processor.getId(), processor.getVersion());
            RuleContext<IN, OUT> rc = createRuleContext(processor, newKey);
            ruleContexts.put(newKey, rc);
            LOG.info("[CEP] Rule update: id=" + newKey.id
                    + (oldKey != null ? ", v" + oldKey.version + " → v" : "")
                    + newKey.version);
        }

        // ③ Handle added (new rule)
        for (PatternProcessor<IN> processor : change.getAdded()) {
            RuleKey newKey = new RuleKey(processor.getId(), processor.getVersion());
            RuleContext<IN, OUT> rc = createRuleContext(processor, newKey);
            ruleContexts.put(newKey, rc);
            LOG.info("[CEP] New rule online: " + newKey);
        }
    }

    /**
     * ★ Handle full rule update (used when TM restarts for recovery).
     *
     * <p>全量规则下发的入口。除"空列表 → 清空所有规则"这一特例外，
     * 其余情况转交 {@link #handleUpdatePatternProcessorEvent} 处理。
     */
    @SuppressWarnings("unchecked")
    private void handleFullUpdate(
            UpdatePatternProcessorEvent<IN> event) throws Exception {
        List<PatternProcessor<IN>> processors = event.getProcessors();

        if (processors == null || processors.isEmpty()) {
            LOG.info("[CEP] Received empty rule list, clearing all NFAs");
            for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                    ruleContexts.entrySet()) {
                pendingStateClear.add(entry.getKey());
                closeRuleContext(entry.getValue());
            }
            ruleContexts.clear();
            return;
        }

        handleUpdatePatternProcessorEvent(event);
    }

    /**
     * ★★★ Multi-rule hot swap core method.
     *
     * <p>Strategy: full replacement. After receiving the new rule list:
     * <ol>
     *   <li>Close NFAs for rules no longer in the new list</li>
     *   <li>Create NFA + independent state for new rules</li>
     *   <li>Mark old rule states for cleanup (deferred until processElement has key context)</li>
     *   <li>Atomically replace the ruleContexts Map</li>
     * </ol>
     *
     * <p>全量热替换（★本类最关键的方法★）。对每条新规则分三种情况处理：
     * <ol>
     *   <li><b>id 与 version 都相同</b> → ★直接复用旧 RuleContext★，
     *       NFA 和已有的部分匹配状态全部原样保留，匹配过程不中断（打印 "Rule kept"）</li>
     *   <li><b>id 相同但 version 变高</b> → 旧版本标记待清理并关闭，再创建新版本；
     *       因为状态名带版本号，新版本的状态必然从零开始</li>
     *   <li><b>id 全新</b> → 直接新建 RuleContext</li>
     * </ol>
     * 随后统一处理"旧 Map 中有、新列表里已没有"的规则（下线），
     * 最后在方法末尾用一行赋值完成 <b>★原子替换★</b>（{@code this.ruleContexts = newContexts}）。
     */
    @SuppressWarnings("unchecked")
    private void handleUpdatePatternProcessorEvent(
            UpdatePatternProcessorEvent<IN> event) throws Exception {

        List<PatternProcessor<IN>> processors = event.getProcessors();

        // Case A: empty rule list → clear all rules
        if (processors == null || processors.isEmpty()) {
            LOG.info("[CEP] Received empty rule list, clearing all NFAs");
            for (RuleContext<IN, OUT> rc : ruleContexts.values()) {
                pendingStateClear.add(new RuleKey(
                        rc.processor.getId(), rc.processor.getVersion()));
                closeRuleContext(rc);
            }
            ruleContexts.clear();
            return;
        }

        // Case B: build new ruleContexts Map
        Map<RuleKey, RuleContext<IN, OUT>> newContexts =
                new HashMap<>(processors.size());

        for (PatternProcessor<IN> processor : processors) {
            RuleKey ruleKey = new RuleKey(
                    processor.getId(), processor.getVersion());

            // ① Check if an old Context with the same id+version already exists (reusable)
            RuleContext<IN, OUT> existing = ruleContexts.get(ruleKey);

            if (existing != null) {
                // Same id+version: reuse (no need to rebuild)
                newContexts.put(ruleKey, existing);
                LOG.info("[CEP] Rule kept: id=" + ruleKey.id
                        + ", version=" + ruleKey.version + " (unchanged)");
                continue;
            }

            // ② Check for old versions to clean up
            RuleKey oldKey = findExistingRuleById(processor.getId());
            if (oldKey != null) {
                RuleContext<IN, OUT> oldRc = ruleContexts.get(oldKey);
                pendingStateClear.add(oldKey);
                closeRuleContext(oldRc);
                LOG.info("[CEP] Rule version update: id=" + ruleKey.id
                        + ", v" + oldKey.version + " → v" + ruleKey.version);
            } else {
                LOG.info("[CEP] New rule online: id=" + ruleKey.id
                        + ", version=" + ruleKey.version);
            }

            // ③ Create new RuleContext (initialize NFA + independent state)
            RuleContext<IN, OUT> rc = createRuleContext(processor, ruleKey);
            newContexts.put(ruleKey, rc);
        }

        // ④ Clean up rules not in the new list
        for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                ruleContexts.entrySet()) {
            if (!newContexts.containsKey(entry.getKey())) {
                LOG.info("[CEP] Rule offline: id=" + entry.getKey().id
                        + ", version=" + entry.getKey().version);
                pendingStateClear.add(entry.getKey());
                closeRuleContext(entry.getValue());
            }
        }

        // ⑤ ★ Atomic replacement
        //    一行赋值完成切换，没有任何中间态：
        //    正在被 processElement 遍历的是旧 Map 引用，不会被并发修改；
        //    本行执行完后，后续事件立刻使用新规则集。这就是"热更新"的落点。
        this.ruleContexts = newContexts;

        LOG.info("[CEP] ★ Rules loaded, currently active: "
                + ruleContexts.size() + "");
        for (RuleKey key : ruleContexts.keySet()) {
            LOG.info("[CEP]   - [" + key.id + "] v" + key.version);
        }
    }

    /**
     * ★ Create a complete runtime context for a single rule:
     *   PatternProcessFunction + JSON→Pattern→NFA + independent ValueState(NFAState)
     *   + independent MapState(eventQueue) + independent InternalTimerService + independent Metrics
     *
     * <p>为单条规则构建完整运行时上下文，共 7 步：
     * <ol>
     *   <li>初始化用户的 PatternProcessFunction：注入 RuntimeContext 并调用 open()</li>
     *   <li>★核心★：JSON → Pattern → NFAFactory → NFA。
     *       注意每次都重新反序列化 Pattern（见 {@code processor.getPattern()}），
     *       因为 Pattern 对象会在 NFA 编译过程中被修改，缓存会导致跨版本污染</li>
     *   <li>创建独立的 ValueState(NFAState)，状态名带 {id}-{version} 后缀</li>
     *   <li>创建独立的 MapState(eventQueue)，同样带后缀</li>
     *   <li>创建★独立的 InternalTimerService★（名字也带后缀）——
     *       这是"每条规则拥有自己时间线"的实现基础</li>
     *   <li>把上一步的定时器服务包装成 CEP 内部使用的 TimerService，供 NFA 调用</li>
     *   <li>创建独立的 Metrics 计数器：迟到事件数 / 命中次数</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    private RuleContext<IN, OUT> createRuleContext(
            PatternProcessor<IN> processor, RuleKey ruleKey) throws Exception {

        String stateSuffix = ruleKey.id + "-" + ruleKey.version;

        // 1) Initialize PatternProcessFunction
        PatternProcessFunction<IN, OUT> function =
                (PatternProcessFunction<IN, OUT>)
                        processor.getPatternProcessFunction();
        if (function != null) {
            FunctionUtils.setFunctionRuntimeContext(
                    function, getRuntimeContext());
            FunctionUtils.openFunction(function, new Configuration());
        }

        // 2) ★ JSON → Pattern → NFAFactory → NFA
        //    规则从"字符串"变成"可执行状态机"的唯一转换点：
        //    GraphSpec JSON → Flink Pattern → NFAFactory → NFA 实例。
        NFACompiler.NFAFactory<IN> nfaFactory =
                NFACompiler.compileFactory(
                        processor.getPattern(getUserCodeClassloader()),
                        function instanceof TimedOutPartialMatchHandler);
        NFA<IN> nfa = nfaFactory.createNFA();
        nfa.open(getRuntimeContext(), new Configuration());

        // 3) ★ Independent ValueState(NFAState) — state name with suffix
        //    保存该规则 NFA 的"计算状态"（当前处于哪些部分匹配分支）。
        //    后缀 {id}-{version} 保证了不同规则 / 不同版本的计算状态物理隔离。
        ValueState<NFAState> computationStates =
                initContext.getKeyedStateStore()
                        .getState(new ValueStateDescriptor<>(
                                NFA_STATE_PREFIX + "-" + stateSuffix,
                                new NFAStateSerializer()));

        // 4) ★ Independent MapState(eventQueue) — state name with suffix
        MapState<Long, List<IN>> elementQueueState =
                initContext.getKeyedStateStore()
                        .getMapState(new MapStateDescriptor<>(
                                EVENT_QUEUE_PREFIX + "-" + stateSuffix,
                                LongSerializer.INSTANCE,
                                new ListSerializer<>(inputSerializer)));

        // 5) ★ Independent InternalTimerService — name with suffix
        //    每条规则一份定时器服务。这是本框架区别于官方 CEP 的关键改动之一：
        //    官方 CEP 一个算子只有一份定时器服务，这里为每条规则各建一份，
        //    因此规则 A 的超时定时器不会影响规则 B 的时间线。
        InternalTimerService<VoidNamespace> timerService =
                getInternalTimerService(
                        TIMER_SERVICE_PREFIX + "-" + stateSuffix,
                        VoidNamespaceSerializer.INSTANCE, this);

        // 6) CEP TimerService wrapper (used internally by NFA)
        TimerService cepTimerService =
                new TimerServiceImpl(timerService);

        // 7) ★ Independent Metrics
        //    指标名同样带 {id}-{version} 后缀，
        //    因此可以在 Flink Web UI 上按规则分别观察 numLateRecordsDropped / patternMatchedTimes。
        Counter lateCounter = metrics.counter(
                "numLateRecordsDropped-" + stateSuffix);
        Counter matchCounter = metrics.counter(
                "patternMatchedTimes-" + stateSuffix);

        return new RuleContext<>(processor, nfa, function,
                computationStates, elementQueueState,
                timerService, cepTimerService,
                lateCounter, matchCounter);
    }

    /**
     * 按规则 id 查找其当前已存在的 RuleKey（不关心版本号）。
     * 用于判断"这条规则是全新的，还是某个已有规则的升级版本"。
     */
    private RuleKey findExistingRuleById(String id) {
        for (RuleKey key : ruleContexts.keySet()) {
            if (key.id.equals(id)) {
                return key;
            }
        }
        return null;
    }

    /**
     * 关闭单条规则的运行时上下文：关闭 NFA、关闭用户函数。
     *
     * <p>★ 注意这里只关闭"内存中的对象"，<b>不清理 keyed state</b>——
     * 状态清理由 {@code pendingStateClear} 延迟到 processElement 中执行。
     * 两处异常都被吞掉并只打 warn，避免一条规则关闭失败导致整个热更新回滚。
     */
    private void closeRuleContext(RuleContext<IN, OUT> rc) {
        try {
            if (rc.nfa != null) {
                rc.nfa.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing NFA", e);
        }
        try {
            if (rc.userFunction != null) {
                FunctionUtils.closeFunction(rc.userFunction);
            }
        } catch (Exception e) {
            LOG.warn("Error closing user function", e);
        }
    }

    // ======================== Phase 5: Rule Lifecycle Management ========================

    /**
     * ★ Handle rule lifecycle events (PAUSE / RESUME / DRAIN / DELETE).
     *
     * <p>规则生命周期控制，四种动作的语义区别：
     * <ul>
     *   <li><b>PAUSE</b>  —— 软暂停：NFA 与状态原样保留，只是不再接收新事件，可随时恢复</li>
     *   <li><b>DRAIN</b>  —— 优雅排空：不再接收新事件，但让已有部分匹配自然走完或超时</li>
     *   <li><b>DELETE</b> —— 硬下线：立即关闭该 id 下<b>所有版本</b>的 NFA 并标记状态清理</li>
     *   <li><b>RESUME</b> —— 从 paused / draining 集合中移除，恢复接收事件</li>
     * </ul>
     *
     * <p>★ 注意：暂停 / 排空只是在集合中记录 id，真正生效是在
     * {@code processElementForRule()} 开头的 {@code shouldSkipRule()} 判断处，
     * 而不是在这里改动任何数据结构——这也是"暂停后还能完整恢复"的原因。
     */
    private void handleLifecycleEvent(RuleLifecycleEvent event) {
        String ruleId = event.getRuleId();

        switch (event.getAction()) {
            case PAUSE:
                pausedRuleIds.add(ruleId);
                LOG.info("[CEP] Rule paused: " + ruleId);
                break;

            case RESUME:
                pausedRuleIds.remove(ruleId);
                drainingRuleIds.remove(ruleId);
                LOG.info("[CEP] Rule resumed: " + ruleId);
                break;

            case DRAIN:
                drainingRuleIds.add(ruleId);
                LOG.info("[CEP] Rule draining: " + ruleId);
                break;

            case DELETE:
                pausedRuleIds.remove(ruleId);
                drainingRuleIds.remove(ruleId);
                java.util.List<RuleKey> toRemove = new java.util.ArrayList<>();
                for (Map.Entry<RuleKey, RuleContext<IN, OUT>> entry :
                        ruleContexts.entrySet()) {
                    if (entry.getKey().id.equals(ruleId)) {
                        pendingStateClear.add(entry.getKey());
                        closeRuleContext(entry.getValue());
                        toRemove.add(entry.getKey());
                    }
                }
                for (RuleKey key : toRemove) {
                    ruleContexts.remove(key);
                }
                LOG.info("[CEP] Rule cleaned up: " + ruleId
                        + " (" + toRemove.size() + " version(s))");
                break;
        }
    }

    /**
     * ★ Check whether a rule should skip event processing (paused or draining)
     *
     * <p>生命周期控制的<b>真正生效点</b>。
     * PAUSE / DRAIN 只是把 id 记进集合，真正让规则"停止吃事件"的是这里的判断。
     * 因为状态与 NFA 都没被销毁，所以 RESUME 后可以立刻从原状态继续。
     */
    private boolean shouldSkipRule(String ruleId) {
        return pausedRuleIds.contains(ruleId) || drainingRuleIds.contains(ruleId);
    }

    // ======================== Inner Classes ========================

    /**
     * ★ Rule runtime context: encapsulates all state and components of a single rule.
     *
     * <p>★单条规则的运行时上下文★，是本类最重要的数据结构。
     *
     * <p>可以理解为："官方 Flink CEP 算子里的那一整套东西，被复制了 N 份，
     * 每一份属于一条规则"——
     * {@code nfa}（匹配状态机）、{@code computationStates}（NFA 计算状态）、
     * {@code elementQueueState}（事件缓冲队列）、{@code timerService}（时间线）、
     * {@code numLateRecordsDropped} / {@code patternMatchedTimes}（指标）、
     * {@code userFunction}（命中回调）。
     *
     * <p>★ 因为每一份都带 {id}-{version} 后缀，所以删除或升级某条规则
     * 不会波及其他规则，这就是"规则之间零耦合"的实现方式。
     */
    private static class RuleContext<IN, OUT> {
        final PatternProcessor<IN> processor;
        final NFA<IN> nfa;
        final PatternProcessFunction<IN, OUT> userFunction;
        final ValueState<NFAState> computationStates;
        final MapState<Long, List<IN>> elementQueueState;
        final InternalTimerService<VoidNamespace> timerService;
        final TimerService cepTimerService;
        final Counter numLateRecordsDropped;
        final Counter patternMatchedTimes;

        RuleContext(
                PatternProcessor<IN> processor,
                NFA<IN> nfa,
                PatternProcessFunction<IN, OUT> userFunction,
                ValueState<NFAState> computationStates,
                MapState<Long, List<IN>> elementQueueState,
                InternalTimerService<VoidNamespace> timerService,
                TimerService cepTimerService,
                Counter numLateRecordsDropped,
                Counter patternMatchedTimes) {
            this.processor = processor;
            this.nfa = nfa;
            this.userFunction = userFunction;
            this.computationStates = computationStates;
            this.elementQueueState = elementQueueState;
            this.timerService = timerService;
            this.cepTimerService = cepTimerService;
            this.numLateRecordsDropped = numLateRecordsDropped;
            this.patternMatchedTimes = patternMatchedTimes;
        }
    }

    /**
     * ★ Rule unique identifier (id + version).
     *
     * <p>规则的唯一标识 = id + 版本号。
     *
     * <p>★ 版本号被纳入"身份"的一部分，这是"规则升级版本后自动启用全新状态"的设计根基：
     * v1 和 v2 是两个不同的 RuleKey，于是它们的 NFA、状态名、定时器、指标名全都不同，
     * 无需任何显式的状态迁移或清理逻辑。
     */
    static class RuleKey {
        final String id;
        final int version;

        RuleKey(String id, int version) {
            this.id = id;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RuleKey)) return false;
            RuleKey r = (RuleKey) o;
            return version == r.version && id.equals(r.id);
        }

        @Override
        public int hashCode() {
            return 31 * id.hashCode() + version;
        }

        @Override
        public String toString() {
            return id + "-v" + version;
        }
    }

    /**
     * OperatorEvent: encapsulates rule list for JM → TM transmission.
     *
     * <p>全量规则下发事件（JM → TM）。
     *
     * <p>用于 subtask 就绪或故障恢复时，把"当前完整规则集"一次性推给 TM。
     * ★ 为什么此时不能用增量？因为重启后的 TM 没有历史基线，
     * 无法判断哪些是新增、哪些是删除，只能用全量对齐。
     */
    public static class UpdatePatternProcessorEvent<IN>
            implements OperatorEvent {
        private static final long serialVersionUID = 1L;
        private final List<PatternProcessor<IN>> processors;

        public UpdatePatternProcessorEvent(
                List<PatternProcessor<IN>> processors) {
            this.processors = new ArrayList<>(processors);
        }

        public List<PatternProcessor<IN>> getProcessors() {
            return processors;
        }
    }

    /**
     * PatternProcessFunction.Context implementation.
     *
     * <p>交给用户 {@code PatternProcessFunction} 使用的上下文对象，
     * 提供时间戳访问与侧输出（OutputTag）能力。
     *
     * <p>★ 注意 {@code currentProcessingTime()} 取的是"第一个活跃规则"的 timerService——
     * 因为所有规则共享同一个物理时钟，取任意一个结果都一样，这里只是借用一下时钟。
     */
    private class ContextFunctionImpl
            implements PatternProcessFunction.Context {
        private Long timestamp;

        @Override
        @SuppressWarnings("unchecked")
        public <X> void output(OutputTag<X> outputTag, X value) {
            StreamRecord<X> record = isProcessingTime
                    ? new StreamRecord<>(value)
                    : new StreamRecord<>(value, timestamp());
            output.collect(outputTag, record);
        }

        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public long currentProcessingTime() {
            // Use the timerService of the first active rule (time is the same for all rules)
            return ruleContexts.isEmpty() ? 0L
                    : ruleContexts.values().iterator().next()
                            .timerService.currentProcessingTime();
        }
    }

    /**
     * TimerService implementation, provides clock access to NFA.
     *
     * <p>适配器：把 Flink 的 {@code InternalTimerService} 包装成
     * CEP 内部使用的 {@code TimerService} 接口，供 NFA 在处理时间语义下读取"当前处理时间"。
     */
    private static class TimerServiceImpl implements TimerService {
        private final InternalTimerService<VoidNamespace> timerService;

        TimerServiceImpl(InternalTimerService<VoidNamespace> timerService) {
            this.timerService = timerService;
        }

        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }
}
