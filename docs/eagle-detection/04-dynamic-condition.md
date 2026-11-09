# eagle-detection 代码精读 · 04 动态条件引擎

> 本篇精读 `cep/condition/AviatorCondition.java`。
> **读完后应能回答**：`"paymentAmount > 50000"` 这样的字符串如何对每条交易事件求值？为什么能比反射快？为什么不能跨事件比较？

---

## 1. 本部分在整体中的位置

```
【第 03 篇】ConditionSpec.AVIATOR
        │  new AviatorCondition<>(expression)
        ▼
【本篇】AviatorCondition
        │  作为 IterativeCondition 传给 Pattern.where()
        ▼
【第 07 篇】NFA 对每条事件调用 condition.filter(event)
```

`AviatorCondition` 是"规则可配置化"的最后一块拼图：
- 第 03 篇解决了 **Pattern 结构**可配置；
- 本篇解决了 **条件表达式**可配置。

没有它，每个新条件的阈值/字段都得写一个 Java 类并重新编译。

---

## 2. 类结构与依赖

```java
public class AviatorCondition<T> extends SimpleCondition<T> {
    private static final long serialVersionUID = 2L;

    // 全局 MethodHandle 缓存：类名 → 字段名 → MethodHandle
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, MethodHandle>> GETTER_CACHE
            = new ConcurrentHashMap<>();
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private final String expression;                    // 表达式字符串（可序列化）
    private transient Expression compiledExpression;    // Aviator 编译产物（不可序列化）
}
```

**继承 `SimpleCondition`**：只依赖当前事件，不需要 `IterativeCondition` 的上下文（这是它能与 Aviator 简单对接的原因）。

---

## 3. 构造与编译

### 3.1 两个构造器

```java
public AviatorCondition(String expression) {
    this(expression, null);          // 不做变量预校验
}

public AviatorCondition(String expression, Class<T> eventClass) {
    this.expression = Objects.requireNonNull(expression, "expression must not be null");
    validateAndCompile();

    if (eventClass != null) {
        preValidateVariables(eventClass);   // 提前发现字段名写错
    }
}
```

| 构造器 | 使用场景 |
|---|---|
| `AviatorCondition(expr)` | 规则 JSON 还原时（不知道事件类，或不想强耦合） |
| `AviatorCondition(expr, EventClass)` | 代码中显式构造，提前校验 |

### 3.2 `validateAndCompile()`

```java
private void validateAndCompile() {
    try {
        AviatorEvaluator.validate(expression);
    } catch (Exception e) {
        throw new IllegalArgumentException("Invalid Aviator expression: " + expression, e);
    }
    this.compiledExpression = AviatorEvaluator.compile(expression, false);
}
```

- `validate`：语法检查，**在构造期**暴露错误（而不是等第一条事件到达）。
- `compile(expression, false)`：第二个参数 `cached=false`，表示不使用 Aviator 的全局缓存——因为每条规则的表达式由自己持有 `Expression`，不需要全局缓存（避免规则版本间的缓存污染）。
- 编译失败抛 `IllegalArgumentException`，会导致规则加载失败。

---

## 4. `filter()`：核心求值

```java
@Override
public boolean filter(T eventBean) throws Exception {
    List<String> variableNames = compiledExpression.getVariableNames();

    if (variableNames.isEmpty()) {
        return true;   // 常量表达式，如 "1 == 1"
    }

    Map<String, Object> variables = new HashMap<>(variableNames.size());
    for (String varName : variableNames) {
        Object value = getVariableValue(eventBean, varName);
        if (value != null) {
            variables.put(varName, value);
        }
    }
    return (Boolean) compiledExpression.execute(variables);
}
```

**执行流程：**

```
事件 eventBean
   │
   ├─ 从 Expression 取出变量名集合（编译期已解析）
   │
   ├─ 对每个变量名，通过 MethodHandle 读取字段值
   │
   ├─ 放入 variables Map
   │
   └─ Aviator 执行表达式 → Boolean
```

**关键设计：**

| 设计 | 原因 |
|---|---|
| 用 `getVariableNames()` 而不是把所有字段塞进去 | 只取表达式用到的字段，减少 MethodHandle 调用与 Map 写入 |
| 值为 null 时不放入 Map | Aviator 中未定义变量按 nil 处理，避免 NPE |
| 返回 `(Boolean)` | 表达式必须返回布尔值；若不是，会 `ClassCastException` |

**边界情况：**

- 表达式只有常量（无变量）→ 直接返回 `true`（例如 `"1 == 1"`；但 `"1 == 2"` 也会错误地返回 `true`！这是实现缺陷）。
- 事件字段为 `null` → 变量缺失，Aviator 按 nil 处理，比较结果为 false（多数情况）。

---

## 5. MethodHandle 访问（性能核心）

### 5.1 `getVariableValue()`

```java
public Object getVariableValue(T bean, String fieldName)
        throws ReflectiveOperationException {

    String className = bean.getClass().getName();

    // ① 读缓存
    ConcurrentHashMap<String, MethodHandle> classCache = GETTER_CACHE.get(className);
    if (classCache != null) {
        MethodHandle mh = classCache.get(fieldName);
        if (mh != null) return invokeHandle(mh, bean);
    }

    // ② 缓存未命中 → 构建
    MethodHandle mh = buildMethodHandle(bean.getClass(), fieldName);
    if (mh == null) {
        throw new NoSuchFieldException(
                "No getter or field '" + fieldName + "' in class " + className);
    }

    // ③ 写入缓存
    GETTER_CACHE.computeIfAbsent(className, k -> new ConcurrentHashMap<>())
                .put(fieldName, mh);

    return invokeHandle(mh, bean);
}
```

**两级缓存结构：**

```
GETTER_CACHE: Map<String className, Map<String fieldName, MethodHandle>>
```

- **static**：所有 `AviatorCondition` 实例共享，跨规则复用。
- **ConcurrentHashMap**：多规则/多线程并发匹配时安全。
- **缓存 key**：类名 + 字段名（不是表达式），所以同一字段在不同规则中只查找一次。

### 5.2 `buildMethodHandle()`

```java
private static MethodHandle buildMethodHandle(Class<?> clazz, String fieldName) {
    // 1) getXxx()
    String getter = "get" + Character.toUpperCase(fieldName.charAt(0))
            + fieldName.substring(1);
    try {
        return LOOKUP.unreflect(clazz.getMethod(getter));
    } catch (NoSuchMethodException | IllegalAccessException e1) { }

    // 2) isXxx()（布尔字段）
    if (!fieldName.startsWith("is")) {
        String isGetter = "is" + Character.toUpperCase(fieldName.charAt(0))
                + fieldName.substring(1);
        try {
            return LOOKUP.unreflect(clazz.getMethod(isGetter));
        } catch (NoSuchMethodException | IllegalAccessException e2) { }
    }

    // 3) 直接字段
    try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return LOOKUP.unreflectGetter(field);
    } catch (NoSuchFieldException | IllegalAccessException e3) {
        return null;
    }
}
```

**查找优先级：getter → is-getter → 直接字段。**

- 只找 `public` 的 `getMethod`；直接字段会 `setAccessible(true)`（JDK 17 下对用户类通常可行）。
- `LOOKUP.unreflect(...)` 把 `Method`/`Field` 转成 `MethodHandle`，之后调用走 JVM 的 invokedynamic 路径，比 `Method.invoke` 少一层反射包装。

### 5.3 `invokeHandle()`

```java
private static Object invokeHandle(MethodHandle mh, Object bean)
        throws ReflectiveOperationException {
    try {
        return mh.invoke(bean);
    } catch (RuntimeException | ReflectiveOperationException e) {
        throw e;
    } catch (Throwable t) {
        throw new ReflectiveOperationException(t);
    }
}
```

`MethodHandle.invoke` 声明抛 `Throwable`，这里统一包装。

### 5.4 性能收益与代价

| 项 | 说明 |
|---|---|
| 收益 | 首次查找后，每条事件只做一次 MethodHandle 调用；避免 `Method.getMethod`/`Field.getDeclaredField` 的重复查找 |
| 代价 | `HashMap` 每事件分配一次（可用线程本地复用优化） |
| 缓存风险 | 若事件类由不同 ClassLoader 加载，同名类会共用缓存 key → **可能方法句柄类型不匹配**（当前场景固定，无问题） |

---

## 6. 变量预校验

```java
private void preValidateVariables(Class<T> eventClass) {
    List<String> variableNames = compiledExpression.getVariableNames();
    for (String varName : variableNames) {
        if (!hasFieldOrGetter(eventClass, varName)) {
            throw new IllegalArgumentException(
                    "Aviator variable '" + varName + "' not found in event class "
                            + eventClass.getName() + "...");
        }
    }
}
```

- 只有在构造时传入 `eventClass` 才会执行。
- 作用：**把"字段名写错"从运行期 NPE 提前到规则加载期**。
- `hasFieldOrGetter` 与 `buildMethodHandle` 的查找顺序一致。

**为什么规则 JSON 还原时不传 eventClass？**
`AviatorConditionSpec.toIterativeCondition(classLoader)` 只拿到表达式，没有事件类型信息（泛型擦除），所以无法校验。这是当前设计的局限。

---

## 7. 序列化处理（关键）

```java
private void readObject(java.io.ObjectInputStream in)
        throws java.io.IOException, ClassNotFoundException {
    in.defaultReadObject();
    validateAndCompile();     // 重新编译
}
```

**为什么需要？**

| 字段 | 是否可序列化 | 处理 |
|---|---|---|
| `expression` | ✅ String | 正常序列化 |
| `compiledExpression` | ❌ Aviator 内部对象 | `transient`，反序列化时重编译 |
| `GETTER_CACHE` / `LOOKUP` | static | 不参与实例序列化 |

**触发场景：**

1. `PatternProcessor` 随 JobGraph 从客户端到 JM；
2. 规则随 `OperatorEvent` 从 JM 到 TM。

如果少了 `readObject` 重编译，反序列化后 `compiledExpression == null`，第一条事件就会 NPE。

> 这也是 `serialVersionUID = 2L`（第 2 版加了 MethodHandle 缓存、改了序列化行为）的原因。

---

## 8. Aviator 语法与限制

### 8.1 常用语法

| 场景 | 写法 |
|---|---|
| 字符串比较 | `action == 'LOGIN_FAIL'` |
| 数值比较 | `paymentAmount > 50000` |
| 逻辑与/或 | `a && b` / `a \|\| b` |
| 取反 | `!flag` |
| 空值判断 | `nil == value` / `value != nil` |
| 三元 | `x > 0 ? 1 : 0` |
| 内置函数 | `string.contains(...)`、`math.abs(...)` 等（取决于 Aviator 版本） |

### 8.2 关键限制

| 限制 | 原因 | 替代方案 |
|---|---|---|
| **不能访问 `previous` / 历史事件** | `SimpleCondition` 只拿到当前事件，Aviator 变量只从当前对象取 | 用 `CLASS` 条件实现 `IterativeCondition`，通过 `ctx.getEventsForPattern()` 访问 |
| 不能引用其他节点的事件 | 同上 | 同上 |
| 字段名必须是 getter 或字段 | 通过 MethodHandle 查找 | 补充 getter |
| `BigDecimal` 与字面量比较 | Aviator 数值类型可能与 `BigDecimal` 不完全兼容 | 优先用 `paymentAmount.compareTo(...)` 或确认 Aviator 数值提升规则 |
| 返回非布尔值 | `(Boolean)` 强转失败 | 确保表达式结果为布尔 |

> ⚠️ **`BigDecimal` 比较是最容易踩的坑**：`paymentAmount > 50000` 在 Aviator 中会做数值提升，但不同版本行为可能有差异，建议在规则上线前用真实事件验证。

---

## 9. 与 CEP 条件的对应关系

```
条件 JSON
 ├─ AVIATOR ──► AviatorCondition（本篇）
 ├─ CLASS   ──► 用户 IterativeCondition（可访问上下文）
 ├─ AND/OR/NOT ──► RichXxxCondition（组合上述条件）
```

`AviatorCondition` 是**叶子条件**，可以被 AND/OR/NOT 包裹。

---

## 10. 本篇小结

### 10.1 一句话总结

`AviatorCondition` 用 Aviator 在运行时编译表达式，用 MethodHandle + 两级全局缓存加速字段读取，用 `transient + readObject` 解决编译产物不可序列化的问题——让条件成为"可配置的数据"。

### 10.2 自测问题

1. `compiledExpression` 为什么必须 `transient`？反序列化时怎么恢复？
2. MethodHandle 缓存为什么用"类名 + 字段名"做 key，而不是表达式？
3. `filter()` 为什么只取 `getVariableNames()` 而不是把事件所有字段都放进去？
4. 为什么 Aviator 条件不能做跨事件比较？替代方案是什么？
5. `preValidateVariables` 在什么情况下不生效？为什么？
6. 表达式 `"1 == 2"` 会返回什么？为什么？

### 10.3 下一篇

→ `05-rule-discovery.md`：规则从哪来、如何检测变更、`initialProcessors` 如何兜底。
