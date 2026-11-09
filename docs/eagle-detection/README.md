# eagle-detection 代码精读系列

> 本目录是 `eagle-detection` 模块的系统性代码精读文档，共 12 篇、约 4800 行。
> 阅读顺序按"学习依赖"编排：先外层后内核，先规则表示后规则流转。

---

## 一、文档清单

| # | 文档 | 行数 | 核心内容 |
|---|---|---|---|
| 00 | [`00-overview.md`](00-overview.md) | ~180 | 8 部分划分、依赖图、阅读顺序、前置知识 |
| 01 | [`01-entry-and-event.md`](01-entry-and-event.md) | ~430 | `DetectionApp` 六步、Kafka/水位、`Transaction`、`DetectionProperties` 单例陷阱 |
| 02 | [`02-api-and-rule-abstraction.md`](02-api-and-rule-abstraction.md) | 463 | `DynamicCEP` 门面、`transform` 选型、`forceNonParallel`、`PatternWrapper`、规则抽象 |
| 03 | [`03-rule-json-serialization.md`](03-rule-json-serialization.md) | 637 | `GraphSpec.toPattern` 逐段、`NodeSpec` 条件/量词还原、Condition 多态树 |
| 04 | [`04-dynamic-condition.md`](04-dynamic-condition.md) | 358 | `AviatorCondition` 编译、MethodHandle 缓存、`transient + readObject` |
| 05 | [`05-rule-discovery.md`](05-rule-discovery.md) | 454 | 三层接口、轮询、`initialProcessors` 兜底、变更检测、function=null 隐患 |
| 06 | [`06-jm-coordinator.md`](06-jm-coordinator.md) | 424 | `computeDelta` 四类判定、增量下发、checkpoint、子任务恢复全量 |
| 07a | [`07a-operator-data-structure.md`](07a-operator-data-structure.md) | 323 | 三重身份、`RuleContext`、状态命名、生命周期方法 |
| 07b | [`07b-operator-event-processing.md`](07b-operator-event-processing.md) | 429 | 广播、EventTime 缓冲回放、`nfa.process`/`advanceTime`、超时 |
| 07c | [`07c-operator-hot-swap.md`](07c-operator-hot-swap.md) | 463 | 三类事件、复用/新建/下线、`createRuleContext` 7 步、原子替换 |
| 07d | [`07d-operator-lifecycle-inner-classes.md`](07d-operator-lifecycle-inner-classes.md) | 317 | PAUSE/RESUME/DRAIN/DELETE、`shouldSkipRule`、5 个内部类 |
| 08 | [`08-end-to-end-and-testing.md`](08-end-to-end-and-testing.md) | 355 | 全链路、`CepTestDemo` 场景 A–F、调试方法、7 个实验 |

**配套文档：**

- [`../eagle-detection-flink-cep-study.md`](../eagle-detection-flink-cep-study.md)：Flink CEP 基础原理 + 模块整体学习指南
- [`../eagle-dynamic-cep.md`](../eagle-dynamic-cep.md)：规则 JSON 字段、运行配置、REST API、FAQ

---

## 二、推荐阅读顺序

```
00 总纲
 └ 01 入口与事件
    └ 02 API 与规则抽象
       ├ 03 JSON 序列化
       ├ 04 动态条件
       └ 05 规则发现
          └ 06 JM 协调器
             └ 07a → 07b → 07c → 07d（执行引擎）
                └ 08 端到端与测试
```

**不建议跳读的原因：**

- 03 的 `GraphSpec.toPattern` 依赖 02 的 `PatternProcessor`；
- 07c 的热更新依赖 06 的事件格式；
- 07b 的时间语义依赖 01 的水位设置。

---

## 三、贯穿全系列的两条主线

### 主线一：规则即数据

```
DefaultPatternProcessor（持 JSON 字符串）
        │ getPattern()
        ▼
CepJsonUtils → GraphSpec → NodeSpec → ConditionSpec
        │
        ▼
Flink Pattern（临时对象，不缓存）
        │ NFACompiler
        ▼
NFA（每规则一个实例）
```

解释了**为什么规则能热更新**：规则不是编译期写死的 Java 对象，而是可序列化、可传输、可重新解析的 JSON 数据。

### 主线二：每规则一套状态

```
RuleKey(id, version)
   │
   ├─ nfaState-{id}-{version}        ValueState<NFAState>
   ├─ eventQueue-{id}-{version}      MapState<Long, List<IN>>
   ├─ watermark-callbacks-{id}-{version}  InternalTimerService
   └─ 指标名同样带后缀
```

解释了**为什么多规则能互不干扰**：状态名带版本后缀，升版后天然从零开始；热替换时未变更规则直接复用。

---

## 四、已知问题汇总

精读过程中确认的 8 个问题（详细分析见对应篇章）：

| # | 问题 | 影响 | 相关文档 |
|---|---|---|---|
| 1 | `ApiPatternProcessorDiscoverer` 热更新规则 `function = null` | 命中时 `processMatchedSequences` NPE | 02 / 05 / 07b |
| 2 | `computeDelta` 只看 `version` 递增 | 内容变更但未升版不会生效 | 06 |
| 3 | API 返回空数组会生成"全部删除" | 可能误清所有规则 | 06 |
| 4 | 生命周期事件无下发入口、`RuleState` 未使用 | 暂停/排空能力实际不可用 | 05 / 06 / 07d |
| 5 | `pausedRuleIds` 等集合不参与 checkpoint | 重启后暂停/排空状态丢失 | 07a |
| 6 | State TTL 未实际配置 | 孤儿状态无法及时回收 | 07a / 07c |
| 7 | `env.execute()` 被 try-catch 吞异常 | 作业可能静默失败 | 01 |
| 8 | 无按规则粒度的容错 | 一条坏规则导致整批更新失败 | 07c |

---

## 五、交付与提交记录

| Commit | 内容 | 变更 |
|---|---|---|
| `d9e2bdb` | 00 总纲 + 01 入口与事件模型 | +770 |
| `074e9c4` | 02 API 与规则抽象 | +463 |
| `591950b` | 03–08 共 9 篇 | +3774 |
| 本次 | 新增本 README 导读 | — |

---

## 六、如何使用

1. **系统学习**：从 `00-overview.md` 开始，按第二节顺序读。
2. **带着问题查**：直接看第四节的问题表，跳到对应篇章。
3. **对照源码**：每篇都给出类/方法定位，建议边读边打开源码。
4. **动手验证**：`08-end-to-end-and-testing.md` 提供 7 个实验清单。
5. **自测**：每篇末尾都有"自测问题"，答不上来就回读。

---

## 七、源码范围

本系列只覆盖 `eagle-detection` 模块（39 个主类 + 4 个测试类），不涉及 `eagle-api`、`eagle-alert`、`eagle-common`、`eagle-log`。

所有结论以源码为准；无法从源码确认的业务语义标注【待确认业务】。
