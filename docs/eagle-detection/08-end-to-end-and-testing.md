# eagle-detection 代码精读 · 08 端到端串讲与测试

> 本篇把 01–07 串成完整链路，并结合 4 个测试示例做端到端分析，最后给出调试方法与实验清单。
> **读完后应能回答**：一条交易事件从 Kafka 到告警经历了什么？怎么验证规则正确？出问题怎么排查？

---

## 1. 端到端全链路

```
┌───────────────────── 客户端（main） ─────────────────────┐
│ DynamicCEP.dynamicPatterns(...)                          │
│   └ 创建 DynamicCepOperatorFactory                        │
└──────────────────────────┬───────────────────────────────┘
                           │ JobGraph
┌──────────────────────────▼───────────────────────────────┐
│ JobManager                                                │
│  DynamicCepCoordinator                                    │
│    ├ ApiPatternProcessorDiscoverer（Timer 30s）          │
│    ├ computeDelta → OperatorEvent                         │
│    └ checkpointCoordinator → 规则列表                     │
└──────────────────────────┬───────────────────────────────┘
                           │ OperatorEvent
┌──────────────────────────▼───────────────────────────────┐
│ TaskManager: DynamicCepOperator                          │
│                                                          │
│  KafkaSource → Transaction.fromString → Watermark        │
│        │                                                 │
│        ▼                                                 │
│  processElement                                          │
│    ├ pendingStateClear                                   │
│    └ for 每条规则 → processElementForRule                │
│           ├ ProcessingTime → 立即匹配                    │
│           └ EventTime → bufferEvent                      │
│                                                          │
│  onEventTime → processTimedEvents → 升序回放              │
│        │                                                 │
│        ▼                                                 │
│  nfa.process / nfa.advanceTime                           │
│        │                                                 │
│        ▼                                                 │
│  processMatchedSequences → userFunction.processMatch     │
│        │                                                 │
│        ▼                                                 │
│  STDOUT [ALERT] ...                                      │
└──────────────────────────────────────────────────────────┘
```

---

## 2. 一条事件的旅程（逐阶段）

以 `Kafka 消息：1001,2026-01-01 10:00:00,7,9,CSH,60000,1767252000000` 为例：

| 阶段 | 组件 | 动作 |
|---|---|---|
| ① 消费 | `KafkaSource` | 读出 CSV 字符串 |
| ② 解析 | `Transaction.fromString` | 7 字段 → 对象，时间转 epoch |
| ③ 水位 | `WatermarkStrategy` | 用 `eventTime` 生成水位 |
| ④ 进入算子 | `DynamicCepOperator.processElement` | 清理待清状态 |
| ⑤ 广播 | `processElementForRule` | 对每条规则调用 |
| ⑥ 时间判定 | EventTime 分支 | `eventTime > watermark` → 缓冲 |
| ⑦ 触发 | `onEventTime` | 水位推进后回放 |
| ⑧ 匹配 | `nfa.process` | 状态转移，产生命中 |
| ⑨ 输出 | `processMatchedSequences` | 调用户函数 |
| ⑩ 告警 | `DetectionAlertFunction` | 格式化 `[ALERT] ...` |

---

## 3. `CepTestDemo` 精读

这是**最适合入门**的文件：自包含、无需 Kafka/MySQL，规则内嵌。

### 3.1 运行模型

```java
List<PatternProcessor<UserBehaviorEvent>> rules = buildRulesStatic();
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setParallelism(1);

DataStream<UserBehaviorEvent> eventStream = env
        .fromCollection(testEvents)
        .assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehaviorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getEventTime()));

PatternProcessorDiscovererFactory<UserBehaviorEvent> factory =
        new ApiPatternProcessorDiscovererFactory<>(
                "http://localhost:9080/eagle-api", rules, 30000L);

SingleOutputStreamOperator<String> cepResults = DynamicCEP.dynamicPatterns(
        eventStream, factory, false, TypeInformation.of(String.class));

cepResults.print("CEP-MATCH").setParallelism(1);
env.execute("Eagle Dynamic CEP Test Demo");
```

**要点：**

- 用 `initialProcessors` 模式：首次轮询直接返回内嵌规则，**不会真的请求 HTTP**。
- `fromCollection` 是有界流，水位会快速推进到结束。
- `false` = EventTime。

### 3.2 四条示例规则

| 规则 | 业务场景 | CEP 结构 |
|---|---|---|
| `rule_login_bruteforce` | 3 次登录失败后 5 分钟内成功 | `LOOPING times(3,3)` → `SINGLE`，5min 窗口 |
| `rule_view_to_order` | 浏览后 30 分钟内下单 | 2 节点 `SKIP_TILL_NEXT`，30min |
| `rule_account_hijack` | 登录 → 改密 → 大额转账(>5万) | 3 节点链 + Aviator 金额条件 |
| `rule_payment_retry` | 支付失败(余额不足 OR 超时)后 2 分钟内成功 | OR 复合条件 → 成功 |

### 3.3 场景与预期

| 场景 | 用户 | 事件序列 | 预期 | 考察点 |
|---|---|---|---|---|
| A | Alice | 3×LOGIN_FAIL → LOGIN_SUCCESS（5min 内） | ✅ 命中规则1 | 循环 + 窗口 |
| B | Bob | VIEW_PRODUCT → PLACE_ORDER（30min 内） | ✅ 命中规则2 | 宽松连续 |
| C | Charlie | LOGIN → CHANGE_PASSWORD → TRANSFER(60000) | ✅ 命中规则3 | 多节点 + 金额条件 |
| D | Dave | PAYMENT_FAIL(BALANCE) → PAYMENT_SUCCESS（2min） | ✅ 命中规则4 | OR 条件 |
| E | Eve | 2×LOGIN_FAIL → LOGIN_SUCCESS | ❌ 不命中 | 次数不足（负向） |
| F | Frank | 3×LOGIN_FAIL → LOGIN_SUCCESS（10min 后） | ❌ 不命中 | 窗口超时（负向） |

**负向用例是理解 CEP 的关键**：

- Eve 证明 `times(3,3)` 是**恰好 3 次**；
- Frank 证明 `within(5min)` 是**首末事件总时长**（FIRST_AND_LAST）。

### 3.4 规则 JSON 与 Flink API 的对应

以规则 1 为例：

```json
{
  "name": "login_bruteforce",
  "type": "COMPOSITE",
  "nodes": [
    {
      "name": "login_fail",
      "quantifier": {
        "consumingStrategy": "STRICT",
        "properties": ["LOOPING"],
        "times": {"from": 3, "to": 3, "windowTime": null}
      },
      "condition": {"type": "AVIATOR", "expression": "action == 'LOGIN_FAIL'"}
    },
    {
      "name": "login_success",
      "quantifier": {"consumingStrategy": "SKIP_TILL_NEXT", "properties": ["SINGLE"]},
      "condition": {"type": "AVIATOR", "expression": "action == 'LOGIN_SUCCESS'"}
    }
  ],
  "edges": [{"source": "login_fail", "target": "login_success", "type": "SKIP_TILL_NEXT"}],
  "window": {"type": "FIRST_AND_LAST", "size": 5, "unit": "MINUTES"},
  "afterMatchStrategy": {"type": "NO_SKIP"}
}
```

等价的 Flink API：

```java
Pattern.<UserBehaviorEvent>begin("login_fail")
    .where(new AviatorCondition<>("action == 'LOGIN_FAIL'"))
    .times(3).consecutive()
    .followedBy("login_success")
    .where(new AviatorCondition<>("action == 'LOGIN_SUCCESS'"))
    .within(Time.minutes(5));
```

> 注意：JSON 中 `times.from=3, to=3` + `properties=["LOOPING"]`，而 `NodeSpec` 对 `LOOPING` 调用的是 `timesOrMore(3)`；对 `TIMES` 才调用 `times(from,to)`。这里 `to` 实际未被使用——**规则定义时应使用 `TIMES` 表示固定次数**，示例里用 `LOOPING` 是写法上的不一致【待确认业务】。

---

## 4. `CepApiIntegrationDemo` 精读

与 `CepTestDemo` 的差异：启动时**真的**调用 `GET /cep/rules`。

```java
List<PatternProcessor<UserBehaviorEvent>> initialRules = fetchRulesFromApi(apiBaseUrl);
if (initialRules.isEmpty()) {
    initialRules = CepTestDemo.buildRulesStatic();   // 降级
}

PatternProcessorDiscovererFactory<UserBehaviorEvent> discovererFactory =
        new ApiPatternProcessorDiscovererFactory<>(apiBaseUrl, initialRules, 30_000L);
```

**演示了完整的两种加载：**

1. 启动同步加载（`fetchRulesFromApi`）；
2. 运行时轮询（`ApiPatternProcessorDiscovererFactory`）。

**注意**：热更新后的规则 `function` 为 null（第 05 篇的 bug），所以该 demo 在规则变化后命中会 NPE。

---

## 5. `DemoCepOutputFunction` 与 `UserBehaviorEvent`

### 5.1 `DemoCepOutputFunction`

```java
public class DemoCepOutputFunction extends PatternProcessFunction<UserBehaviorEvent, String> {
    private final String ruleId;
    private final int version;

    public DemoCepOutputFunction(String ruleId, int version) { ... }

    @Override
    public void processMatch(Map<String, List<UserBehaviorEvent>> match,
                             Context ctx, Collector<String> out) {
        // 打印规则 id + 各节点事件明细
    }
}
```

- 构造函数 `(String, int)`：与 JDBC Discoverer 的反射约定一致；
- `match` 的 key 是节点名。

### 5.2 `UserBehaviorEvent`

测试事件类，字段：`userId/action/product/amount/reason/eventTime`。

**与生产 `Transaction` 的差异：**

| 方面 | `Transaction` | `UserBehaviorEvent` |
|---|---|---|
| 金额类型 | `BigDecimal` | `double` |
| 时间字段 | `eventTime` (long) | `eventTime` (long) |
| 用途 | 生产 | 测试示例 |

---

## 6. 调试方法与常见问题

### 6.1 日志关键点

| 日志 | 含义 |
|---|---|
| `[API-DISCOVER] Using initial rules` | 首次使用启动规则 |
| `[API-DISCOVER] ✅ Rule change detected (N rules)` | 检测到规则变化 |
| `[COORD] Incremental changes: +a ~r ↑u` | 增量计算完成 |
| `[CEP] ★ Rules loaded, currently active: N` | TM 规则加载完成 |
| `[CEP] Rule kept / New rule online / Rule offline` | 热更新分支 |
| `[CEP] ⚠ Event arrived but no active rules` | 事件先于规则到达 |
| `numLateRecordsDropped` 指标 | 迟到事件数 |
| `patternMatchedTimes` 指标 | 命中数 |

### 6.2 常见问题排查

| 现象 | 可能原因 | 排查 |
|---|---|---|
| 无任何输出 | 规则未加载 | 看 `[CEP] ★ Rules loaded` |
| 有规则但无命中 | 时间窗口/水位 | 检查事件时间、`transaction.out.of.orderness` |
| 规则更新不生效 | version 未递增 | 看 API 返回的 version |
| 热更新后命中报 NPE | `function == null` | 看 `ApiPatternProcessorDiscoverer` |
| 窗口超时不回调 | 未实现 `TimedOutPartialMatchHandler` | 查看用户函数 |
| 迟到事件多 | 乱序容忍太小 | 调大 `transaction.out.of.orderness` |
| 吞吐上不去 | CEP 单并行度 | 见 `forceNonParallel` |

### 6.3 断点调试建议

在以下位置打断点，观察状态：

1. `DynamicCepOperator.processElement`：看事件是否到达、规则数；
2. `processElementForRule`：看时间语义分支；
3. `processTimedEvents`：看缓冲与回放；
4. `processEvent`：看 `nfa.process` 返回的匹配；
5. `createRuleContext`：看规则编译、状态名。

---

## 7. 动手实验清单

| # | 实验 | 目的 | 做法 | 预期 |
|---|---|---|---|---|
| 1 | 缩短窗口 | 理解 `within` | 把规则1 `size:5` 改 `1` | Frank 仍不命中；若把成功事件提前到 4min 内则命中 |
| 2 | 改连续性 | 理解 Contiguity | 边类型改 `STRICT` | 中间有无关事件时不再命中 |
| 3 | 改量词 | 理解 Quantifier | `LOOPING` 改 `TIMES`，`from=2,to=3` | 2 或 3 次失败均可 |
| 4 | 加规则 | 理解多规则并行 | 复制规则4，改节点名 | 同一事件可命中多条规则 |
| 5 | 热更新 | 理解动态 CEP | 启动后改 API 规则并升 version | 无需重启生效 |
| 6 | 负向用例 | 理解匹配边界 | 去掉 Frank 的成功事件 | 不命中 |
| 7 | 断点观察 NFA | 理解底层 | 在 `processEvent` 看 `nfaState` | 观察 partialMatches 变化 |

---

## 8. 全链路依赖关系总览

```
DetectionApp
 ├─ DetectionProperties ── Constant
 ├─ Transaction
 ├─ ApiPatternProcessorDiscovererFactory
 │    └ ApiPatternProcessorDiscoverer
 │         └ DefaultPatternProcessor
 │              └ CepJsonUtils → GraphSpec → NodeSpec → ConditionSpec
 │                   └ AviatorCondition
 └─ DynamicCEP
      └ DynamicCepOperatorFactory
           ├ CoordinatorProvider → DynamicCepCoordinator
           │    ├ PatternProcessorChangeEvent
           │    └ RuleLifecycleEvent
           └ DynamicCepOperator
                ├ RuleContext
                ├ RuleKey
                ├ ContextFunctionImpl
                └ TimerServiceImpl
```

---

## 9. 本系列总结

### 9.1 一句话概括

`eagle-detection` 用"JSON 定义规则 + Discoverer 发现变更 + Coordinator 计算增量并下发 + Operator 多 NFA 热替换"四层协作，把 Flink CEP 从编译期静态模式改造成运行时可配置引擎。

### 9.2 关键设计回顾

| 设计 | 解决的问题 |
|---|---|
| JSON 作为规则载体 | Pattern 不可变、不可序列化 |
| `{id}-{version}` 状态名 | 版本隔离、升版从零 |
| 全量/增量双通道 | 恢复对齐 vs 日常最小更新 |
| `pendingStateClear` | RPC 线程无 key 上下文 |
| 每规则独立 TimerService | 超时互不干扰 |
| `initialProcessors` 兜底 | 启动期规则就绪 |

### 9.3 已知问题汇总

1. `ApiPatternProcessorDiscoverer` 热更新规则 `function=null` → 命中 NPE。
2. `computeDelta` 只看 version 递增，内容变更不升版会失效。
3. `latest` 为空数组会清空所有规则（无法区分 API 异常）。
4. 生命周期事件无下发入口，`RuleState` 未使用。
5. `pausedRuleIds` 等集合不参与 checkpoint，重启丢失。
6. State TTL 未配置，孤儿状态无法及时回收。
7. `env.execute()` 被 try-catch 吞异常。
8. 无按规则的容错，一条坏规则导致整批更新失败。

### 9.4 自测问题（综合）

1. 画出从 Kafka 到告警的完整链路，标出每一步的类。
2. 规则升版时，旧状态会立即被删除吗？为什么？
3. 为什么 TM 重启后要发全量规则？
4. EventTime 下事件乱序到达，如何保证匹配顺序正确？
5. 如何新增一条规则并让它在一个轮询周期内生效？
6. 热更新规则命中会 NPE，根因和修复方向是什么？

---

## 10. 参考

- [`00-overview.md`](00-overview.md)：总纲与阅读顺序
- [`../eagle-detection-flink-cep-study.md`](../eagle-detection-flink-cep-study.md)：Flink CEP 基础与整体学习指南
- [`../eagle-dynamic-cep.md`](../eagle-dynamic-cep.md)：规则 JSON 字段、运行配置、REST API、FAQ
- Flink CEP 官方文档：https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/libs/cep/
