# eagle-detection 代码精读 · 07a 执行引擎：数据结构与生命周期

> 本篇是 `DynamicCepOperator` 四篇中的第一篇，聚焦**数据结构、状态布局、算子生命周期**。
> **读完后应能回答**：算子内部如何表示"多条规则"？每条规则持有哪些状态？状态名为什么带版本后缀？

---

## 1. 类的三重身份

```java
public class DynamicCepOperator<IN, KEY, OUT>
        extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>,
                   Triggerable<KEY, VoidNamespace>,
                   OperatorEventHandler {
```

| 身份 | 来自接口 | 能力 | 对应方法 |
|---|---|---|---|
| 数据流算子 | `OneInputStreamOperator` | 处理事件 | `processElement` |
| 定时器回调 | `Triggerable` | 响应事件/处理时间 | `onEventTime` / `onProcessingTime` |
| 事件接收者 | `OperatorEventHandler` | 接收 JM 规则变更 | `handleOperatorEvent` |

**为什么不用 `ProcessFunction`？** 见第 02 篇：需要自定义状态、定时器、事件通道，只有 `AbstractStreamOperator` 能给。

---

## 2. 字段全景

### 2.1 构造参数（序列化字段）

```java
private final boolean isProcessingTime;
private final TypeSerializer<IN> inputSerializer;
```

| 字段 | 作用 |
|---|---|
| `isProcessingTime` | 时间语义分支开关 |
| `inputSerializer` | 构造 `MapState<Long, List<IN>>` 时必需 |

### 2.2 核心运行时结构

```java
private transient Map<RuleKey, RuleContext<IN, OUT>> ruleContexts;
private transient SharedBuffer<IN> sharedPartialMatches;
```

| 字段 | 作用 |
|---|---|
| `ruleContexts` | **中央注册表**：规则 → 运行时上下文 |
| `sharedPartialMatches` | 所有规则共用的部分匹配缓冲区 |

`transient`：这些是运行时对象，由 Flink 状态恢复机制重建，不参与算子对象序列化。

### 2.3 延迟清理与生命周期集合

```java
private transient final List<RuleKey> pendingStateClear = new ArrayList<>();
private transient final java.util.Set<String> pausedRuleIds = new java.util.HashSet<>();
private transient final java.util.Set<String> drainingRuleIds = new java.util.HashSet<>();
```

| 字段 | 作用 | 为什么是 transient |
|---|---|---|
| `pendingStateClear` | 待清理的旧规则 key | 运行时队列（不持久化，重启后由规则列表重建） |
| `pausedRuleIds` | 暂停的规则 id | 同上 |
| `drainingRuleIds` | 排空中的规则 id | 同上 |

> ⚠️ 注意：这三个集合**不参与 checkpoint**，作业恢复后会丢失。当前设计下，重启后所有规则会重新加载为 ACTIVE，暂停/排空状态不会保留——这是一个已知的行为特征【待确认业务】。

### 2.4 运行时组件

```java
private transient TimestampedCollector<OUT> collector;
private transient ContextFunctionImpl context;
private transient StateInitializationContext initContext;
```

| 字段 | 作用 |
|---|---|
| `collector` | 输出收集器（支持设置时间戳） |
| `context` | 传给用户 `PatternProcessFunction` 的上下文 |
| `initContext` | 保存状态初始化上下文，供后续 `createRuleContext` 创建状态 |

**`initContext` 为什么必须保存？**
`createRuleContext` 是在 `handleOperatorEvent`（运行时）被调用的，此时已经没有 `initializeState` 的参数，必须持有 `initContext` 才能调用 `getKeyedStateStore().getState(...)`。

### 2.5 状态名前缀常量

```java
private static final String NFA_STATE_PREFIX = "nfaState";
private static final String EVENT_QUEUE_PREFIX = "eventQueue";
private static final String TIMER_SERVICE_PREFIX = "watermark-callbacks";
```

实际状态名 = `前缀-{规则id}-{版本号}`，例如：

```
nfaState-rule_login_bruteforce-1
eventQueue-rule_login_bruteforce-1
watermark-callbacks-rule_login_bruteforce-1
```

---

## 3. `RuleContext`：单规则运行时上下文（核心）

```java
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
}
```

| 成员 | 类型 | 保存什么 | 对应原生 CEP 的 |
|---|---|---|---|
| `processor` | 规则定义 | id/version/JSON/function | — |
| `nfa` | `NFA<IN>` | 编译后的状态机（运行时对象） | `CepOperator.nfa` |
| `userFunction` | `PatternProcessFunction` | 命中回调 | `CepOperator.userFunction` |
| `computationStates` | `ValueState<NFAState>` | 部分匹配/已完成匹配 | `CepOperator.computationStates` |
| `elementQueueState` | `MapState<Long, List<IN>>` | 按时间戳缓冲的事件 | `CepOperator.elementQueueState` |
| `timerService` | `InternalTimerService` | 事件/处理时间定时器 | `CepOperator.timerService` |
| `cepTimerService` | `TimerService` | 给 NFA 用的时钟适配 | `TimerServiceImpl` |
| `numLateRecordsDropped` | `Counter` | 迟到事件计数 | 同名 |
| `patternMatchedTimes` | `Counter` | 命中次数 | 同名 |

**一句话**：把原生 `CepOperator` 的整套成员，复制 N 份，每份绑定一个 `RuleKey`。

### 3.1 为什么 `nfa` 不是状态

`NFA` 是**编译产物**（由 JSON 还原的 Pattern 编译而来），不是需要持久化的数据。
需要持久化的是 `NFAState`（`computationStates`）——它记录了"NFA 当前走到了哪些状态"。

因此恢复时：
1. 重新从规则 JSON 编译 NFA（通过 `processor.getPattern()`）；
2. 从 `ValueState` 读回 `NFAState`。

### 3.2 `RuleKey`

```java
static class RuleKey {
    final String id;
    final int version;

    @Override public boolean equals(Object o) {
        if (!(o instanceof RuleKey)) return false;
        RuleKey r = (RuleKey) o;
        return version == r.version && id.equals(r.id);
    }

    @Override public int hashCode() {
        return 31 * id.hashCode() + version;
    }
}
```

**版本号进入身份**，是"升版即新状态"的根基（见 3.3）。

### 3.3 状态版本隔离（核心机制）

```
规则 v1:  nfaState-rule_x-1   eventQueue-rule_x-1   timer-rule_x-1
规则 v2:  nfaState-rule_x-2   eventQueue-rule_x-2   timer-rule_x-2
                 ↑ 完全不同的状态名
```

**效果：**

| 场景 | 行为 |
|---|---|
| 规则升版 | 新版本读写新状态 → **从零开始匹配** |
| 旧版本清理 | 旧状态成为孤儿，靠延迟清理/TTL 回收 |
| 多规则共存 | 不同 id 天然隔离 |
| 回滚到旧版本 | 若旧状态未被清理，可复用（当前清理策略下通常已清） |

**代价**：每次升版都会产生一批新状态；若频繁升版且 TTL 未配置，状态会累积。

---

## 4. 生命周期方法

### 4.1 `setup()`

```java
@Override
public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
    super.setup(containingTask, config, output);
}
```

仅转调父类（由工厂在 `createStreamOperator` 中调用）。这里保留重写是为了显式表达生命周期，实际无额外逻辑。

### 4.2 `initializeState()`

```java
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.initContext = context;

    // 创建共享 SharedBuffer
    this.sharedPartialMatches = new SharedBuffer<>(
            context.getKeyedStateStore(),
            inputSerializer,
            SharedBufferCacheConfig.of(getOperatorConfig().getConfiguration()));

    this.ruleContexts = new HashMap<>();
}
```

**关键点：**

1. **只建空容器**，不加载规则——规则由 `OperatorEvent` 推送。
2. `SharedBuffer` 需要 `KeyedStateStore`（它在内部使用 keyed state 存部分匹配）。
3. `SharedBufferCacheConfig.of(...)` 从算子配置读缓存参数。
4. `ruleContexts = new HashMap<>()`：非线程安全，但只在算子线程访问（RPC 线程通过事件队列串行化），可接受。

**为什么规则不在这里加载？**
因为 `initializeState` 拿不到规则列表（规则在 JM 的 Coordinator 里）。Flink 的启动顺序是：TM 初始化状态 → 注册事件处理器 → Coordinator 发事件。所以规则只能在后续事件中到达。

### 4.3 `open()`

```java
@Override
public void open() throws Exception {
    super.open();
    collector = new TimestampedCollector<>(output);
    context = new ContextFunctionImpl();
}
```

- `TimestampedCollector`：输出时可携带时间戳（EventTime 下需要）。
- `ContextFunctionImpl`：用户函数上下文。

**注意**：此时 `ruleContexts` 仍为空；规则要等事件。

### 4.4 `close()`

```java
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
```

- 逐条关闭 NFA 与用户函数；
- 释放 SharedBuffer 的缓存统计定时器（避免线程泄漏）。

---

## 5. 状态布局总览

```
KeyedStateBackend
├── (key = NullByte / 用户 key)
│   ├── nfaState-{id}-{version}          ValueState<NFAState>
│   ├── eventQueue-{id}-{version}        MapState<Long, List<IN>>
│   └── 定时器命名空间 watermark-callbacks-{id}-{version}
│
└── SharedBuffer 内部使用的 keyed state（部分匹配条目）
```

**注意：以上状态都是 per-key 的。**
由于 `DetectionApp` 用 `NullByteKeySelector`，实际上只有一个 key（0），所以状态是"全局一份"。如果调用方自己 keyBy，则每个 key 各有一份。

---

## 6. 设计要点与陷阱

| 点 | 说明 |
|---|---|
| 规则不在 `initializeState` 加载 | 依赖 Coordinator 事件，存在"规则未到先丢事件"窗口 |
| `initContext` 必须保存 | 运行时 `createRuleContext` 需要创建状态 |
| 状态名带版本 | 版本隔离的关键 |
| `nfa` 是运行时对象 | 不入状态，恢复时重新编译 |
| `NFAState` 入状态 | 记录匹配进度 |
| 三个集合不持久化 | 重启后暂停/排空状态丢失 |
| `SharedBuffer` 全规则共享 | 已知局限，靠状态名区分 |

**易错点：**

1. 认为 `ruleContexts` 会在启动时就有内容——不会，要看日志 `[CEP] ★ Rules loaded`。
2. 认为规则状态会随 checkpoint 完整恢复——规则定义在 Coordinator 侧恢复，TM 侧靠补发全量。
3. 在 `initializeState` 里读状态——此时 `ruleContexts` 为空，读不到。

---

## 7. 本篇小结

### 7.1 一句话总结

`DynamicCepOperator` 用一个 `Map<RuleKey, RuleContext>` 承载 N 条规则，每条规则拥有独立的 NFA、ValueState、MapState、TimerService 和 Metrics；状态名带 `{id}-{version}` 后缀实现版本隔离；算子初始化时刻意不加载规则，等待 Coordinator 事件。

### 7.2 自测问题

1. 算子的三重身份分别来自哪个接口？各对应哪个方法？
2. 为什么 `initContext` 必须保存下来？
3. `nfa` 为什么不放进状态？恢复时如何重建？
4. `computationStates` 保存的是什么？
5. 状态名为什么带版本后缀？升版后旧状态怎么办？
6. `pausedRuleIds` 等集合不参与 checkpoint 会导致什么？
7. 为什么 `initializeState` 不加载规则？

### 7.3 下一篇

→ `07b-operator-event-processing.md`：事件如何广播给所有规则、EventTime 如何缓冲与回放、超时如何触发。
