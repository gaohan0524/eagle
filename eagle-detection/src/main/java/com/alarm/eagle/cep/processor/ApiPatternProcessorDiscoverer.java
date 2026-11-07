package com.alarm.eagle.cep.processor;

import com.alarm.eagle.cep.json.CepJsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ★ HTTP-based rule discoverer — polls eagle-api for CEP rules.
 *
 * <p>Replaces {@link JDBCPeriodicPatternProcessorDiscoverer} with HTTP polling,
 * fetching rules from eagle-api's {@code GET /cep/rules} endpoint.
 *
 * <p>Rule JSON format from API:
 * <pre>{@code
 * [
 *   {
 *     "id": "rule_1",
 *     "version": 1,
 *     "pattern": "{...GraphSpec JSON...}",
 *     "function": null
 *   }
 * ]
 * }</pre>
 *
 * <p>业务含义：生产环境实际使用的发现器，替代 JDBC 直连方式，
 * 通过 HTTP 轮询 eagle-api 的 {@code GET /cep/rules} 获取规则。
 * 支持 initialProcessors 兜底（API 不可达时先用启动期加载的规则）。
 *
 * @param <T> event type
 */
public class ApiPatternProcessorDiscoverer<T>
        extends PeriodicPatternProcessorDiscoverer<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ApiPatternProcessorDiscoverer.class);

    private final String apiBaseUrl;
    private final ClassLoader userCodeClassLoader;
    private final List<PatternProcessor<T>> initialProcessors;
    private List<PatternProcessor<T>> latestProcessors;

    private static final ObjectMapper mapper = CepJsonUtils.getObjectMapper();

    /**
     * @param apiBaseUrl           eagle-api 基础 URL
     * @param userCodeClassLoader  ClassLoader
     * @param initialProcessors    初始规则
     * @param intervalMillis       轮询间隔
     */
    public ApiPatternProcessorDiscoverer(
            String apiBaseUrl,
            ClassLoader userCodeClassLoader,
            @Nullable List<PatternProcessor<T>> initialProcessors,
            Long intervalMillis) {
        super(intervalMillis);
        this.apiBaseUrl = Objects.requireNonNull(apiBaseUrl);
        this.userCodeClassLoader = userCodeClassLoader;
        this.initialProcessors = initialProcessors;
    }

    /**
     * 检测规则是否发生变更。
     *
     * <p>业务含义：首次轮询优先使用 initialProcessors（保证事件到达前规则已就绪）；
     * 之后轮询 API 并比较"规则 id→version 映射"是否变化（只认版本号，不看内容）。
     *
     * @return {@code true} 表示规则有变更，需要调用 {@link #getLatestPatternProcessors()} 拉取
     */
    @Override
    public boolean arePatternProcessorsUpdated() {
        // First run: use initial rules if available
        if (latestProcessors == null
                && initialProcessors != null
                && !initialProcessors.isEmpty()) {
            LOG.info("[API-DISCOVER] Using initial rules (total: "
                    + initialProcessors.size() + ")");
            return true;
        }

        try {
            String rulesUrl = apiBaseUrl + "/cep/rules";
            LOG.info("[API-DISCOVER] Polling: GET " + rulesUrl);
            String json = httpGet(rulesUrl);
            if (json == null || json.isEmpty()) {
                LOG.info("[API-DISCOVER] ⚠ Empty response from API");
                return false;
            }

            JsonNode root = mapper.readTree(json);
            // API 返回 Response 包装: { "code": 0, "data": [...] }
            JsonNode dataNode = root.has("data") ? root.get("data") : root;

            List<PatternProcessor<T>> current = new ArrayList<>();
            if (dataNode.isArray()) {
                for (JsonNode item : dataNode) {
                    String ruleId = item.get("ruleId").asText();
                    int version = item.get("version").asInt();
                    String patternStr = item.get("pattern").asText();
                    String functionClassName = item.has("function")
                            && !item.get("function").isNull()
                            ? item.get("function").asText() : null;

                    DefaultPatternProcessor<T> pp = new DefaultPatternProcessor<>(
                            ruleId, version, patternStr, null);
                    current.add(pp);
                }
            }

            if (latestProcessors == null || isChanged(current)) {
                LOG.info("[API-DISCOVER] ✅ Rule change detected (" +
                        current.size() + " rules)");
                latestProcessors = current;
                return true;
            }
            LOG.info("[API-DISCOVER] No changes (" +
                    current.size() + " rules)");
            return false;

        } catch (Exception e) {
            LOG.warn("Failed to check rule changes from API: {}", e.getMessage());
            LOG.info("[API-DISCOVER] ⚠ API query failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * 返回最新规则列表。
     *
     * <p>业务含义：返回一份新的列表副本（避免调用方修改影响缓存）；
     * 首次且无 API 数据时回退返回 initialProcessors。
     *
     * @return 最新规则列表
     * @throws IllegalStateException 在未先调用 {@link #arePatternProcessorsUpdated()} 且无初始规则时抛出
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<PatternProcessor<T>> getLatestPatternProcessors()
            throws Exception {
        if (latestProcessors == null && initialProcessors != null
                && !initialProcessors.isEmpty()) {
            latestProcessors = new ArrayList<>();
            LOG.info("[API-DISCOVER] Returning initial rules (total: "
                    + initialProcessors.size() + ")");
            return new ArrayList<>(initialProcessors);
        }
        if (latestProcessors == null) {
            throw new IllegalStateException(
                    "latestProcessors is null. Call arePatternProcessorsUpdated() first.");
        }
        return new ArrayList<>(latestProcessors);
    }

    private boolean isChanged(List<PatternProcessor<T>> current) {
        if (latestProcessors == null) return true;
        if (latestProcessors.size() != current.size()) return true;

        java.util.Map<String, Integer> oldMap = latestProcessors.stream()
                .collect(Collectors.toMap(
                        PatternProcessor::getId, PatternProcessor::getVersion));
        java.util.Map<String, Integer> newMap = current.stream()
                .collect(Collectors.toMap(
                        PatternProcessor::getId, PatternProcessor::getVersion));
        return !oldMap.equals(newMap);
    }

    private String httpGet(String urlStr) throws IOException {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.setRequestProperty("Accept", "application/json");

            int code = conn.getResponseCode();
            if (code != 200) {
                LOG.info("[API-DISCOVER] HTTP " + code + " from " + urlStr);
                return null;
            }

            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream(),
                            StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            }
            return sb.toString();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
}
