# eagle-detection 模块与 Flink CEP 系统学习指南

> **读者**：想系统掌握 `eagle-detection` 模块代码、以及它所依赖的 Flink CEP 原理的工程师。
> **配套文档**：本文偏"原理 + 代码讲解 + 学习路径"；运行配置、规则 JSON 字段参考、REST API、FAQ 见 [`eagle-dynamic-cep.md`](eagle-dynamic-cep.md)。
> **源码口径**：本文所有结论均以 `eagle-detection` 模块源码为准；无法从源码确认的业务语义会标注【待确认业务】。

---

## 目录

1. [第一部分：Flink CEP 基础（前置知识）](#第一部分flink-cep-基础)
2. [第二部分：eagle-detection 模块总览](#第二部分eagle-detection-模块总览)
3. [第三部分：核心代码逐层讲解](#第三部分核心代码逐层讲解)
4. [第四部分：Dynamic CEP 的核心原理（难点）](#第四部分dynamic-cep-的核心原理)
5. [第五部分：设计权衡与已知局限](#第五部分设计权衡与已知局限)
6. [第六部分：学习路径与动手实验](#第六部分学习路径与动手实验)
7. [附录](#附录)

---

# 第一部分：Flink CEP 基础

`eagle-detection` 的 Dynamic CEP 直接建立在 Flink CEP 之上，并深入使用了它的内部类（`NFA`、`NFAState`、`SharedBuffer`、`NFACompiler`）。因此**必须先理解 Flink CEP 本身**，才能看懂本模块。

## 1.1 什么是 CEP

**CEP（Complex Event Processing，复杂事件处理）** 是从**事件流**中识别**有顺序/时间关系的组合事件**的技术。

| 对比项 | 普通流处理（map/filter/window） | CEP |
|---|---|---|
| 关注点 | 单条事件，或按 key 聚合的窗口统计 | **多条事件之间的顺序与时间关系** |
| 典型问题 | "每 5 分钟交易总额是多少" | "同一用户 3 次登录失败后 5 分钟内成功" |
| 输出 | 聚合结果 | 命中的**事件序列**（pattern match） |

典型应用场景：欺诈检测、风控、异常检测、设备故障预测、用户行为分析。

## 1.2 Pattern（模式）API

Flink CEP 用**链式 API** 描述一个事件序列模式：

```java
Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
    .where(e -> e.getAction().equals("LOGIN_FAIL"))
    .times(3).consecutive()                       // 连续 3 次失败
    .followedBy("end")
    .where(e -> e.getAction().equals("LOGIN_SUCCESS"))
    .within(Time.minutes(5));                     // 整体 5 分钟内
```

模式由**节点（Pattern）+ 条件 + 量词 + 连接关系 + 时间窗口**组成。

## 1.3 连续性（Contiguity / ConsumingStrategy）

控制**模式之间**事件如何"连接"，是 CEP 最容易混淆的概念之一。

| 策略 | API | 含义 |
|---|---|---|
| `STRICT` | `next()` | 严格连续：中间不能有任何其他事件 |
| `SKIP_TILL_NEXT` | `followedBy()` | 宽松连续：跳过不匹配事件，向后找 |
| `SKIP_TILL_ANY` | `followedByAny()` | 非确定宽松：允许任意组合，可能产生多种匹配 |
| 否定 | `notNext()` / `notFollowedBy()` | 要求某事件**不**出现 |

示例（事件 `A1 A2 B A3`，匹配 `A` 三次）：

```
STRICT          : A2 与 A3 之间有 B，匹配失败 ❌
SKIP_TILL_NEXT  : 跳过 B，A1→A2→A3 匹配成功 ✅
SKIP_TILL_ANY   : 同上，且支持多种组合路径 ✅
```

## 1.4 量词（Quantifier）

控制单个节点"匹配多少次"：

| API | 含义 |
|---|---|
| `times(n)` | 恰好 n 次 |
| `times(from, to)` | from 到 to 次 |
| `oneOrMore()` / `timesOrMore(n)` | 至少 1 次 / n 次 |
| `optional()` | 可选（可出现可不出现） |
| `greedy()` | 贪婪：尽可能多匹配 |
| `consecutive()` / `allowCombinations()` | 循环**内部**的连续/组合策略 |

## 1.5 条件（Condition）

| 类型 | 说明 |
|---|---|
| `SimpleCondition` | 只看当前事件，实现 `filter(event)` |
| `IterativeCondition` | 可访问上下文（`ctx.getEventsForPattern(...)` 看历史），实现 `filter(event, ctx)` |
| 复合条件 | `where().or()`、`RichAndCondition`、`RichOrCondition`、`RichNotCondition` |
| `until(cond)` | 循环终止条件 |

## 1.6 时间窗口（within）

`within(Time)` 给整个模式加时间约束，超时则部分匹配被清理（可配合超时回调）。

两种计算方式（`WithinType`）：

| 类型 | 含义 |
|---|---|
| `FIRST_AND_LAST`（默认） | 首事件到末事件的总时长 ≤ 窗口 |
| `PREVIOUS_AND_CURRENT` | 每两个相邻匹配事件的间隔 ≤ 窗口 |

## 1.7 命中后跳过策略（AfterMatchSkipStrategy）

一次命中后，如何继续在流中寻找下一次匹配：

| 策略 | 含义 |
|---|---|
| `noSkip()` | 返回所有可能匹配（可能大量重复） |
| `skipPastLastEvent()` | 从最后一个事件之后继续 |
| `skipToNext()` | 从第一个事件之后继续 |
| `skipToFirst(name)` / `skipToLast(name)` | 跳到命名节点的第一个/最后一个事件 |

## 1.8 匹配输出与超时处理

```java
// 命中输出
public class Fn extends PatternProcessFunction<Event, String> {
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) {
        // match: key = 节点名, value = 该节点命中的事件列表
    }
}

// 超时输出（需额外实现 TimedOutPartialMatchHandler）
public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) {
    // 部分匹配超时（未走完模式就超 window）
}
```

## 1.9 Flink CEP 底层原理（重点）

理解这些内部机制，是读懂 `DynamicCepOperator` 的前提。

### 1.9.1 Pattern → NFA 编译

`NFACompiler.compileFactory(pattern, ...)` 把 Pattern 链编译成 **NFA（非确定性有限自动机）**。

- NFA 由 **State（状态）** 和 **Transition（转移）** 组成。
- State 有类型：`Start`（起点）、`Normal`（中间）、`Final`（终点）。
- Transition 的 action 类型：
  - `TAKE`：事件满足条件，进入下一个状态；
  - `IGNORE`：事件不满足条件，停留在当前状态；
  - `PROCEED`：ε 转移（无条件推进，如可选节点）。

### 1.9.2 匹配过程：ComputationState + 版本

- 一个 **ComputationState** = `(当前状态, 版本号)`，代表一条"进行中的部分匹配路径"。
- 每条事件到达时，NFA 从**所有活跃的 ComputationState** 出发尝试转移，产生新的 ComputationState。
- 到达 `Final` 状态即产生一次完整匹配（completed match）。

### 1.9.3 SharedBuffer（共享缓冲区）

- 存储所有**部分匹配序列**，是 CEP 的核心状态存储。
- 用 **SharedBufferEntry**（事件 + 引用计数）组织，支持版本控制与垃圾回收（引用计数归零即删除）。
- 同一个事件可被多条部分匹配路径共享，避免重复存储。

### 1.9.4 NFAState

NFAState 是 CEP 算子的**键控状态**，包含：
- `partialMatches`：进行中的部分匹配集合；
- `completedMatches`：已完成的匹配。

### 1.9.5 事件时间语义下的处理

原生 CEP 算子在 EventTime 下：
1. 事件先进入 `elementQueueState`（按时间戳缓冲），注册定时器；
2. watermark 推进时，按时间戳升序**回放**缓冲事件给 NFA；
3. 迟于 watermark 的事件被丢弃（`numLateRecordsDropped`）。

> 这套"缓冲 + 回放"逻辑，`DynamicCepOperator` 里**每条规则复制了一份**（见 3.9）。

---

# 第二部分：eagle-detection 模块总览

## 2.1 模块定位

`eagle-detection` 是 Eagle 平台的**交易欺诈检测 Flink 作业**。它的核心价值不在"用 CEP"，而在 **把 Flink CEP 从"编译期写死 Pattern"改造成"运行时可热更新规则"**——即 **Dynamic CEP**。

一句话概括它解决的问题：

> 原生 Flink CEP 的 `Pattern` 必须在提交作业前用 Java 代码写好；`eagle-detection` 让规则变成 JSON，作业运行期间可以**新增/修改/删除规则而无需重启**。

## 2.2 目录结构

```
eagle-detection/src/main/java/com/alarm/eagle/
├── DetectionApp.java            # ★ Flink 作业入口
├── config/
│   ├── Constant.java            # 配置 key 常量
│   └── DetectionProperties.java # 多环境参数装配
├── message/
│   └── Transaction.java         # 交易事件 DTO（CEP 匹配的事件类型）
└── cep/
    ├── DynamicCEP.java          # ★ 对外 API 门面
    ├── PatternWrapper.java      # 暴露 Pattern 受保护构造器的桥接类
    ├── condition/
    │   └── AviatorCondition.java          # 动态表达式条件
    ├── json/
    │   ├── CepJsonUtils.java              # Pattern ↔ JSON 转换
    │   ├── TimeDeserializer.java          # 窗口时长反序列化
    │   └── spec/                          # 13 个 JSON 序列化模型
    │       ├── GraphSpec.java             # 整条规则的图表示
    │       ├── NodeSpec.java              # 单节点
    │       ├── ConditionSpec.java + 5 子类 # 条件（AVIATOR/CLASS/OR/AND/NOT）
    │       ├── QuantifierSpec.java        # 量词
    │       ├── TimesSpec.java             # 次数
    │       ├── WindowSpec.java            # 时间窗口
    │       ├── EdgeSpec.java              # 边（节点连接）
    │       └── AfterMatchSkipStrategySpec.java
    ├── operator/
    │   ├── DynamicCepOperator.java        # ★★ TM 侧执行引擎（核心）
    │   ├── DynamicCepOperatorFactory.java # 算子工厂
    │   ├── DynamicCepCoordinator.java     # ★ JM 侧规则协调器
    │   ├── DynamicCepCoordinatorProvider.java
    │   ├── PatternProcessorChangeEvent.java# 增量变更事件
    │   └── RuleLifecycleEvent.java        # 生命周期事件
    └── processor/
        ├── PatternProcessor.java          # 规则抽象接口
        ├── DefaultPatternProcessor.java   # 规则实现
        ├── PatternProcessorDiscoverer.java       # 发现器接口
        ├── PatternProcessorDiscovererFactory.java
        ├── PatternProcessorManager.java          # 回调接口
        ├── PeriodicPatternProcessorDiscoverer.java # 周期轮询基类
        ├── ApiPatternProcessorDiscoverer.java     # HTTP 轮询（生产用）
        ├── ApiPatternProcessorDiscovererFactory.java
        ├── JDBCPeriodicPatternProcessorDiscoverer.java # JDBC 轮询（备选）
        ├── JDBCPeriodicPatternProcessorDiscovererFactory.java
        └── RuleState.java                # 规则生命周期枚举（当前未使用）
```

测试示例（非生产，用于本地学习验证）：

```
src/test/java/com/alarm/eagle/cep/example/
├── CepTestDemo.java            # 自包含测试：4 条规则 + 模拟事件
├── CepApiIntegrationDemo.java  # 与 eagle-api 联调示例
├── DemoCepOutputFunction.java  # 命中输出函数
└── UserBehaviorEvent.java      # 测试事件类
```

## 2.3 核心数据流

```
Kafka(topic=eagle-transaction)
   → KafkaSource(SimpleStringSchema)
   → Transaction.fromString()                # CSV 解析
   → assignTimestampsAndWatermarks()         # EventTime + 乱序容忍
   → DynamicCEP.dynamicPatterns()            # 门面
        └ DynamicCepOperator                 # 每条事件广播给所有规则的 NFA
   → DetectionAlertFunction.processMatch()   # 命中格式化
   → STDOUT print("CEP-ALERT")
```

**三条并行的逻辑线**（学习时要分清）：

| 逻辑线 | 执行位置 | 作用 |
|---|---|---|
| 流处理主流程 | TM `DynamicCepOperator` | 事件 → NFA 匹配 → 输出 |
| 定时任务（规则轮询） | JM `ApiPatternProcessorDiscoverer`（Timer 线程） | 周期拉取规则、检测变更 |
| 异步规则下发 | JM→TM `OperatorEvent`（RPC） | 规则增删改热更新 |

---

# 第三部分：核心代码逐层讲解

按"从入口到引擎核心"的顺序，逐层深入。

## 3.1 入口 `DetectionApp`

主类，6 步组装作业（对应 `main()` 方法）：

1. `getStreamExecutionEnvironment()`：创建环境，dev 用 `createLocalEnvironment`（并行度 1），prod 用 `getExecutionEnvironment`；可选开 checkpoint。
2. `getTransactionStream()`：Kafka 源 + CSV 解析 + 水位。
3. `fetchRulesFromApi()`：**启动时同步** HTTP `GET /cep/rules` 拉规则 → `initialProcessors`（保证事件到达前规则就绪）。
4. 构造 `ApiPatternProcessorDiscovererFactory`（带 initialRules + 轮询间隔）。
5. `DynamicCEP.dynamicPatterns(...)`：进入 Dynamic CEP。
6. `cepResults.print("CEP-ALERT")`：输出告警。

**关键学习点**：

- 第 3 步"同步加载"与第 4 步"后台轮询"是**两套规则获取机制**，前者兜底、后者热更新，缺一不可。
- 水位用 `forBoundedOutOfOrderness(Duration.ofMillis(orderness))`，时间戳取 `txn.getEventTime()`——这是 CEP `within` 窗口正确性的前提。
- 告警输出当前仅 `print` 到 STDOUT，代码注释提到的 Kafka sink **未落地**【待确认业务】。

## 3.2 事件模型 `Transaction`

CEP 处理的事件类型（`IN`），从 CSV 解析。字段即 Aviator 表达式可引用的"事件字段"：

| 字段 | 业务含义 |
|---|---|
| `transactionId` | 交易唯一 ID |
| `eventTime` | 交易发生时间（事件时间，水位依据） |
| `payerId` | 付款方 ID |
| `beneficiaryId` | 收款方 ID |
| `paymentAmount` | 交易金额（金额阈值条件的字段） |
| `paymentType` | 支付类型（取值枚举源码未定义）【待确认业务】 |
| `ingestionTimestamp` | 接入时间戳（是否参与匹配）【待确认业务】 |

> Aviator 表达式通过 **getter** 访问字段，因此字段命名 = 规则可引用的变量名。

## 3.3 API 门面 `DynamicCEP`

对齐官方 `CEP.pattern()` 的调用习惯，屏蔽底层协调器细节。核心方法 `dynamicPatterns`：

```java
DynamicCEP.dynamicPatterns(input, discovererFactory, isProcessingTime, outTypeInfo)
```

**关键行为**（易踩坑）：

- 若 `input` 已是 `KeyedStream` 直接复用；否则 `keyBy(NullByteKeySelector)` + **`forceNonParallel()`**。
- 意味着 `DetectionApp`（传的是非 KeyedStream）里，**CEP 算子被强制单并行度**——这是"所有事件进同一 key 同一 subtask"的设计选择，也是吞吐瓶颈。

## 3.4 规则抽象 `PatternProcessor` / `DefaultPatternProcessor`

一条规则 = `id + version + patternJSON + function`。

- `PatternProcessor` 接口：规则的可序列化抽象。
- `DefaultPatternProcessor`：持有 **JSON 字符串**而非 `Pattern` 对象。设计理由（类注释自述）：
  1. `Pattern` 在 NFA 编译时会被修改（注入 untilCondition 等）；
  2. JSON 是纯数据，天然可序列化，支持 JM→TM RPC 传输；
  3. 各版本独立，无 Pattern 对象变异副作用。
- `getPattern(classLoader)`：**每次调用都重新** `CepJsonUtils.convertJSONStringToPattern` 反序列化，避免跨版本污染。

## 3.5 规则序列化 `GraphSpec` 体系（`cep/json/spec`）

这是"规则可配置化"的核心：规则以 JSON 存储，运行时还原成 Flink `Pattern`。

```
Pattern ──(GraphSpec.fromPattern)──► GraphSpec ──(Jackson)──► JSON
JSON   ──(Jackson)──► GraphSpec ──(GraphSpec.toPattern)──► Pattern
```

### 3.5.1 `GraphSpec`：把 Pattern 链建模成有向图

字段：`nodes`、`edges`、`window`、`afterMatchStrategy`。

`toPattern()`（JSON→Pattern，规则加载时调用）核心流程：
1. 建 `nodeCache` / `edgeCache` 索引；
2. `findBeginPatternName()`：找**入度为 0** 的节点作起点（要求唯一）；
3. 沿边逐节点 `NodeSpec.toPattern(...)` 串链；
4. 嵌套 `GraphSpec`（COMPOSITE）用 `buildGroupConnection` 按边类型映射 `next/followedBy/followedByAny`；
5. 最后 `prevPattern.within(window.getTime(), window.getType())`。

### 3.5.2 `NodeSpec`：还原单节点语义

`toPattern()` 依次恢复：
1. 用 `PatternWrapper` 调 Pattern 受保护构造器；
2. 条件：`where`（或 `or`）；
3. 量词：`optional` / `greedy` / `timesOrMore` / `times`；
4. 内部连续性：`allowCombinations` / `consecutive`；
5. `until(condition)`。

### 3.5.3 `ConditionSpec`：多态条件树

通过 `@JsonTypeInfo(property="type")` 多态，5 种类型：

| type | 还原逻辑 |
|---|---|
| `AVIATOR` | `new AviatorCondition<>(expression)` |
| `CLASS` | `classLoader.loadClass(className).newInstance()` |
| `OR` / `AND` | 递归折叠构造 `RichOrCondition` / `RichAndCondition` |
| `NOT` | `new RichNotCondition<>(sub)` |

### 3.5.4 `PatternWrapper`：绕过访问限制

Flink `Pattern` 构造器是 `protected`，外部包无法 `new`。`PatternWrapper` 继承它并用 `super()` 暴露公共构造器，避免反射的脆弱性。

## 3.6 动态条件 `AviatorCondition`

规则里写 `"action == 'LOGIN_FAIL'"` 这种**字符串表达式**，运行时对每条事件求值，无需编译 Java 类。要点：

- **构造时校验+编译**：`AviatorEvaluator.validate` + `compile`。
- **`compiledExpression` 是 `transient`**：Aviator 编译产物不可序列化，`readObject` 里重新编译（因为规则要跨节点传输）。
- **MethodHandle 缓存**：静态 `GETTER_CACHE`（类名 → 字段名 → MethodHandle），查找优先 `getXxx()` → `isXxx()` → 直接字段；比反射 `invoke()` 快 10–30%。
- **变量预校验**：可选传 `eventClass`，构造时检查表达式变量是否都有 getter/字段，把配置错误提前暴露。
- `filter()` 只取表达式实际引用的变量（`getVariableNames()`）求值。

> 局限：Aviator 只能访问**当前事件**字段，无法用 `previous` 做跨事件比较（见 1.5，跨事件用 CLASS 条件 + IterativeCondition）。

## 3.7 规则发现 `Discoverer` 体系

接口三层：`PatternProcessorDiscovererFactory`（工厂，需 Serializable）→ `PatternProcessorDiscoverer`（发现器）→ `PatternProcessorManager`（回调接收者）。

`PeriodicPatternProcessorDiscoverer` 是基类，用 `java.util.Timer` 定时调度，子类只需实现：
- `arePatternProcessorsUpdated()`：检测是否有变更；
- `getLatestPatternProcessors()`：拉取最新规则列表。

两个实现：

| 实现 | 数据源 | 变更判定 | 状态 |
|---|---|---|---|
| `ApiPatternProcessorDiscoverer` | HTTP `GET /cep/rules` | 比较 `Map<id, version>` | **生产用** |
| `JDBCPeriodicPatternProcessorDiscoverer` | `SELECT * FROM table` | 同 id 取最大 version，比较 RowData | 备选，未启用 |

两者都支持 **`initialProcessors` 兜底**：首次轮询若外部源不可达/为空，返回启动期加载的规则。

> ⚠️ `TimerTask` 内必须 catch 所有异常，否则未捕获异常会终止整个 Timer 线程，规则轮询彻底停止（`java.util.Timer` 的已知陷阱）。

## 3.8 JM 侧协调器 `DynamicCepCoordinator`

运行在 JobManager，实现 `OperatorCoordinator` + `PatternProcessorManager`。职责：

1. `start()`：创建 Discoverer，注册本协调器为回调接收者；
2. `onPatternProcessorsUpdated()`：Discoverer 回调入口，计算增量并广播；
3. `computeDelta()`：**全量列表 → 增量（added/removed/updated）**；
4. `sendToAllGateways()`：通过 `SubtaskGateway.sendEvent()` 下发；
5. `checkpointCoordinator()` / `resetToCheckpoint()`：规则列表随 JM 状态持久化；
6. `executionAttemptReady()` / `subtaskReset()`：TM 重启后补发。

**关键设计**：
- 增量事件（`PatternProcessorChangeEvent`）用于日常变更，**最小化 RPC 与重建开销**；
- 子任务重启/恢复时发**全量** `UpdatePatternProcessorEvent`（因为重启的 TM 没有历史基线，无法算增量）。

## 3.9 TM 侧引擎 `DynamicCepOperator`（★★ 核心）

这是整个模块最重的类（同时实现 `OneInputStreamOperator` + `Triggerable` + `OperatorEventHandler`），把"官方 CEP 算子的能力"复制成**每规则一份**。

### 3.9.1 核心数据结构：`Map<RuleKey, RuleContext>`

```java
Map<RuleKey, RuleContext<IN, OUT>> ruleContexts;   // 中央注册表
```

- `RuleKey = (id, version)`：规则唯一标识，版本号纳入身份。
- `RuleContext`：单条规则的完整运行时上下文，含：
  - `nfa`：匹配状态机；
  - `computationStates`：`ValueState<NFAState>`（NFA 计算状态）；
  - `elementQueueState`：`MapState<Long, List<IN>>`（事件缓冲）；
  - `timerService`：独立 `InternalTimerService`（时间线）；
  - `numLateRecordsDropped` / `patternMatchedTimes`：独立指标；
  - `userFunction`：命中回调。

**一句话**：官方 Flink CEP 算子里的"那一整套东西"被复制了 N 份，每份属于一条规则。

### 3.9.2 状态版本隔离（关键机制）

所有状态名带 `{id}-{version}` 后缀：

```
nfaState-{id}-{version}
eventQueue-{id}-{version}
watermark-callbacks-{id}-{version}
metrics: numLateRecordsDropped-{id}-{version}
```

**效果**：规则升版后新 NFA 读写全新状态名，天然从零开始，旧版本状态互不干扰、可被回收。这是"无需显式状态迁移就能热更新"的根基。

### 3.9.3 事件处理：广播给所有规则

`processElement()` 三步：
1. 补做 `pendingStateClear` 延迟清理（此刻才有 key 上下文）；
2. 无规则 → 丢弃事件等待；
3. 遍历 `ruleContexts`，每条规则独立 `processElementForRule`（单规则异常被 catch 隔离）。

`processElementForRule()` 按时间语义分流：
- **ProcessingTime**：立即 `advanceTime` → `processEvent` → `updateNFA`；
- **EventTime**：`timestamp > watermark` 则 `bufferEvent` 缓冲，否则计入 `numLateRecordsDropped`。

### 3.9.4 EventTime 缓冲与回放

`onEventTime()` → 遍历所有规则 → `processTimedEvents()`：把缓冲队列中 `≤ watermark` 的事件按时间戳**升序**回放给 NFA。这保证了**同一 key 内事件严格按时间顺序处理**（乱序到达也能正确匹配）。

`processEvent()` 直接调 Flink NFA 原生 API：

```java
rc.nfa.process(accessor, nfaState, event, timestamp, skipStrategy, rc.cepTimerService);
```

若规则有 `within` 且产生新起点，注册 `timestamp + windowTime` 定时器——这是**超时部分匹配**能被清理/回调的前提。

### 3.9.5 规则热更新（核心中的核心）

`handleOperatorEvent()` 是热更新总入口，分发三类事件：

| 事件 | 场景 | 处理 |
|---|---|---|
| `UpdatePatternProcessorEvent` | 全量（TM 重启恢复） | `handleFullUpdate` |
| `PatternProcessorChangeEvent` | 增量（日常变更） | `handleIncrementalChange`（删→改→增） |
| `RuleLifecycleEvent` | 生命周期 | `handleLifecycleEvent` |

**全量热替换 `handleUpdatePatternProcessorEvent()`**（最核心）对每条新规则分三种情况：

1. **id + version 都相同** → 直接复用旧 `RuleContext`（NFA 与部分匹配状态原样保留，不中断匹配）；
2. **id 相同 version 变高** → 旧版本标记 `pendingStateClear` 并关闭，建新版本（状态从零开始）；
3. **id 全新** → 直接新建。

最后一行 `this.ruleContexts = newContexts;` 完成**原子替换**。

**`createRuleContext()`** 7 步：初始化用户函数 → `JSON→Pattern→NFA` → 建 ValueState → 建 MapState → 建独立 TimerService → 包装 CEP TimerService → 建独立 Metrics。

### 3.9.6 生命周期控制

`RuleLifecycleEvent.Action`：`PAUSE`（软暂停，保留状态）/ `RESUME` / `DRAIN`（优雅排空）/ `DELETE`（硬下线）。

实现细节：PAUSE/DRAIN 只把 id 放进 `pausedRuleIds` / `drainingRuleIds` 集合，**真正生效点**在 `shouldSkipRule()`（`processElementForRule` 开头判断）。

> ⚠️ 注意：`RuleLifecycleEvent` 的**下发入口目前未接通**（Coordinator 的 `handleEventFromOperator` 为空），`RuleState` 枚举也未被代码引用——属于已实现但未启用的能力。

---

# 第四部分：Dynamic CEP 的核心原理

## 4.1 原生 Flink CEP 为什么不支持动态规则

原生 `CEP.pattern()` 有几个"写死"的限制：

1. **Pattern 在提交前用 Java 代码构建**，编译成 NFA 后不可改；
2. **一个 CEP 算子 = 一个 NFA**，状态名固定（`"nfaState"`、`"elementQueueState"`）；
3. **无规则的增删改通道**，改规则必须改代码、重新打包、重启作业。

## 4.2 动态化改造方案（三步）

| 改造点 | 原生 | Dynamic CEP |
|---|---|---|
| 规则载体 | Java 编译的 Pattern | JSON 字符串（`GraphSpec`） |
| NFA 数量 | 1 个 | N 个（`Map<RuleKey, RuleContext>`） |
| 规则来源 | 硬编码 | Discoverer 轮询（HTTP/JDBC） |
| 更新通道 | 无 | OperatorEvent（JM→TM RPC） |
| 状态隔离 | 固定状态名 | 状态名带 `{id}-{version}` 后缀 |

## 4.3 热更新端到端流程（时序）

```
1. JM 启动 → DynamicCepCoordinator.start()
        └ 创建 ApiPatternProcessorDiscoverer（Timer 立即轮询 + 30s 周期）

2. 首次轮询 → arePatternProcessorsUpdated()
        ├ 有 initialProcessors → 直接返回 true
        └ getLatestPatternProcessors() → manager.onPatternProcessorsUpdated(list)

3. Coordinator.onPatternProcessorsUpdated()
        ├ computeDelta(): currentProcessors==null ⇒ 全部 added
        └ sendToAllGateways(PatternProcessorChangeEvent)   // TM 未就绪则 0 gateway

4. TM executionAttemptReady(subtask, gateway)
        └ resendToSubtask() → 发 UpdatePatternProcessorEvent（全量）

5. TM handleOperatorEvent → handleFullUpdate → handleUpdatePatternProcessorEvent
        ├ 同 id+version 复用；新版建 RuleContext；旧版 pendingStateClear + close
        └ ruleContexts = newContexts（原子替换）

6. 事件到达 → processElement
        ├ 先执行 pendingStateClear（有 key 上下文）
        └ 对每条 ACTIVE 规则 processElementForRule → NFA → processMatch → 输出

7. 后台每 30s 再轮询 → 有变更 → computeDelta → 增量事件 → handleIncrementalChange
        └ 只动变更规则，其余 NFA 无感

8. checkpoint → Coordinator 序列化 currentProcessors
   TM 重启 → subtaskReset → 重发全量规则恢复
```

## 4.4 状态版本隔离的原理

这是 Dynamic CEP 能"热更新且不串状态"的核心：

- **RuleKey 含 version** → v1、v2 是两个不同 key → 它们的 NFA、状态、定时器、指标天然不同；
- **状态名含 version 后缀** → 升版后新规则读写全新状态名，旧版本状态成孤儿，靠延迟清理 + State TTL 回收；
- **`getPattern()` 每次重新反序列化** → 避免 Pattern 对象在 NFA 编译中被修改后污染其他版本。

---

# 第五部分：设计权衡与已知局限

源码注释已自述的局限（学习时应重点关注，理解设计者的取舍）：

| 局限 | 说明 | 影响 |
|---|---|---|
| SharedBuffer 全规则共享 | 所有规则共用一个 `SharedBuffer`（未 per-rule 隔离） | 当前靠状态名区分无冲突；要真正隔离需改 Flink 源码 |
| 状态清理只能清当前 key | `pendingStateClear` 在 `processElement` 中执行 | 其他 key 的旧状态成孤儿，靠 State TTL 异步回收 |
| 强制单并行度 | 非 KeyedStream 时 `keyBy(NullByte)` + `forceNonParallel` | 吞吐瓶颈 |
| 状态 TTL 未实际配置 | 注释提到"依赖 State TTL"，但描述符未设置 `StateTtlConfig` | 孤儿状态无法及时回收【待确认业务】 |

代码阅读中发现的实际问题（本学习文档额外标注）：

1. **热更新规则 `function` 为 null**：`ApiPatternProcessorDiscoverer` 解析时读了 `function` 字段但**没用**（构造 `DefaultPatternProcessor` 时传 `null`），而启动期规则带 `DetectionAlertFunction`。热更新后的规则一旦命中，`processMatchedSequences` 里 `userFunction.processMatch` 会 **NPE**。
2. **`computeDelta` 只看 version 递增**：规则内容变了但 version 未增，会被忽略——规则管理端必须保证版本号自增。
3. **`DetectionApp` 的 `env.execute()` 在 try-catch 内**：作业异常被吞，可能静默失败。
4. **`RuleLifecycleEvent` / `RuleState` 未接通**：生命周期控制只落地了 TM 侧逻辑，无对外下发入口。

---

# 第六部分：学习路径与动手实验

## 6.1 分阶段学习路线

**阶段一：跑通一个例子（建立直觉）**

读 `CepTestDemo.java`，重点看 4 条规则的 JSON 定义 + 场景 A–F（含负向用例），运行 `main()` 观察 `CEP-MATCH` 输出。

**阶段二：理解规则如何变成 Pattern**

读 `GraphSpec.toPattern()` → `NodeSpec.toPattern()` → `PatternWrapper`，回答："一条 JSON 规则是怎么变成能跑的 NFA 的"。

**阶段三：理解引擎如何跑规则**

读 `DynamicCepOperator`，按这个顺序：
1. 字段 `ruleContexts` + 内部类 `RuleContext`；
2. `processElement`（广播）；
3. `createRuleContext`（状态名后缀）；
4. `handleUpdatePatternProcessorEvent`（热替换）。

**阶段四：理解规则如何被下发**

读 `DynamicCepCoordinator.computeDelta()` + `ApiPatternProcessorDiscoverer`，串起"发现→增量→下发→热替换"完整链路。

**阶段五：回到 Flink CEP 底层**

结合第一部分，理解 `DynamicCepOperator.processEvent` 里 `nfa.process(...)` 背后的 NFA / SharedBuffer / NFAState 机制。

## 6.2 动手实验清单

| 实验 | 目的 | 做法 |
|---|---|---|
| 改时间窗口 | 理解 within | 把 `RULE1` 的 `size:5` 改成 `1` 分钟，重跑看 Frank 是否命中 |
| 改连续性 | 理解 Contiguity | 把边类型 `SKIP_TILL_NEXT` 改成 `STRICT`，观察匹配差异 |
| 加一条规则 | 理解多规则并行 | 复制 `RULE4` 改成 3 节点链，观察同事件被多规则命中 |
| 热更新 | 理解动态 CEP | 启动 `CepApiIntegrationDemo` 后，运行中改 eagle-api 规则，观察无重启生效 |
| 读源码验证 NFA | 理解底层 | 在 `DynamicCepOperator.processEvent` 打断点，观察 `nfaState.partialMatches` 变化 |

---

# 附录

## A. 关键类/方法速查表

| 类 / 方法 | 职责 | 对应知识点 |
|---|---|---|
| `NFACompiler.compileFactory` | Pattern→NFA | 1.9.1 |
| `NFA.process` | 事件驱动状态转移 | 1.9.2 |
| `SharedBuffer` | 部分匹配存储 | 1.9.3 |
| `DynamicCepOperator.processElement` | 事件广播给所有规则 | 3.9.3 |
| `DynamicCepOperator.createRuleContext` | 单规则上下文（状态隔离） | 3.9.2 |
| `DynamicCepOperator.handleUpdatePatternProcessorEvent` | 全量热替换 | 3.9.5 |
| `DynamicCepCoordinator.computeDelta` | 全量→增量 | 3.8 |
| `GraphSpec.toPattern` | JSON→Pattern | 3.5.1 |
| `AviatorCondition.filter` | 表达式求值 | 3.6 |

## B. 术语对照表

| 英文 | 中文 | 含义 |
|---|---|---|
| Complex Event Processing | 复杂事件处理 | 识别事件序列模式 |
| Pattern | 模式 | 事件序列的定义 |
| NFA | 非确定性有限自动机 | Pattern 编译后的执行状态机 |
| Contiguity / ConsumingStrategy | 连续性 | 事件间连接策略 |
| Quantifier | 量词 | 节点匹配次数 |
| Within | 时间窗口 | 模式总时长约束 |
| AfterMatchSkipStrategy | 命中后跳过策略 | 一次命中后如何继续 |
| Partial Match | 部分匹配 | 未走完模式的中间状态 |
| SharedBuffer | 共享缓冲区 | 部分匹配的存储 |
| Watermark | 水位 | 事件时间推进标志 |
| OperatorEvent | 算子事件 | JM↔TM 通信机制 |
| OperatorCoordinator | 算子协调器 | JM 侧协调逻辑 |

## C. 待确认业务清单

1. `Transaction.paymentType` 的具体取值/枚举。
2. `Transaction.ingestionTimestamp` 是否参与 CEP 匹配。
3. `DetectionApp` 告警输出的 Kafka sink 是否计划实现。
4. 规则 JSON 中 `function` 字段对应的 `PatternProcessFunction` 类来源。
5. 模块内无"告警等级"概念，等级如何与规则关联。
6. 状态 TTL 是否计划实际配置 `StateTtlConfig`。

## D. 参考

- [Flink CEP 官方文档](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/libs/cep/)
- 本仓库 `docs/eagle-dynamic-cep.md`（规则 JSON 字段、运行配置、REST API、FAQ）
- 本仓库 `CLAUDE.md`（项目整体架构与模块分工）
