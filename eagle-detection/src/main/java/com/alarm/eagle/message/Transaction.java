package com.alarm.eagle.message;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * 交易事件模型 —— Dynamic CEP 匹配用事件类。
 *
 * <p>在实时流中的定位：从 Kafka 原始 CSV 字符串解析出的标准交易事件，
 * 是 CEP 算子（{@code DynamicCepOperator}）唯一处理的事件类型（{@code IN}）。
 *
 * <p>CEP 规则条件通过 Aviator 表达式访问本类的 getter（如
 * {@code payerId}、{@code paymentAmount}），因此字段命名与 getter 即为规则可引用的"事件字段"。
 *
 * <p>事件时间语义：{@code eventTime} 是交易发生时间，被 {@code WatermarkStrategy} 提取为
 * 事件时间戳，是 CEP 时间窗口（within）与乱序判定的依据。
 */
public class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 交易唯一标识，用于告警时定位具体交易 */
    private long transactionId;
    /** 交易发生时间（事件时间，epoch 毫秒），CEP 时间语义与水位的依据 */
    private long eventTime;
    /** 付款方账户 id（交易主体，可用于按账户维度关联事件） */
    private long payerId;
    /** 收款方账户 id（交易对手方） */
    private long beneficiaryId;
    /** 交易金额，CEP 金额阈值条件（如 {@code amount > 50000}）的目标字段 */
    private BigDecimal paymentAmount;
    /** 支付方式/交易类型（具体枚举取值未在本模块定义）【待确认业务】 */
    private String paymentType;
    /** 数据接入/入库时间戳（处理时间侧参考），是否参与 CEP 匹配【待确认业务】 */
    private Long ingestionTimestamp;

    private static final transient DateTimeFormatter timeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withLocale(Locale.US).withZone(ZoneOffset.UTC);

    public Transaction() {}

    /** 从 CSV 行解析: transactionId,eventTime,payerId,beneficiaryId,paymentType,paymentAmount,ingestionTimestamp */
    public static Transaction fromString(String line) {
        List<String> tokens = Arrays.asList(line.split(","));
        if (tokens.size() != 7) {
            throw new RuntimeException(
                    "Invalid transaction: " + line
                            + ". Required 7 fields, found " + tokens.size());
        }

        Transaction transaction = new Transaction();
        try {
            Iterator<String> iter = tokens.iterator();
            transaction.transactionId = Long.parseLong(iter.next());
            transaction.eventTime = ZonedDateTime.parse(iter.next(), timeFormatter)
                    .toInstant().toEpochMilli();
            transaction.payerId = Long.parseLong(iter.next());
            transaction.beneficiaryId = Long.parseLong(iter.next());
            transaction.paymentType = iter.next();
            transaction.paymentAmount = new BigDecimal(iter.next());
            transaction.ingestionTimestamp = Long.parseLong(iter.next());
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return transaction;
    }

    // ======================== Getters (Aviator 表达式通过 getter 访问) ========================

    public long getTransactionId() { return transactionId; }
    public long getEventTime() { return eventTime; }
    public long getPayerId() { return payerId; }
    public long getBeneficiaryId() { return beneficiaryId; }
    public BigDecimal getPaymentAmount() { return paymentAmount; }
    public String getPaymentType() { return paymentType; }
    public Long getIngestionTimestamp() { return ingestionTimestamp; }

    // ======================== Setters ========================

    public void setTransactionId(long transactionId) { this.transactionId = transactionId; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    public void setPayerId(long payerId) { this.payerId = payerId; }
    public void setBeneficiaryId(long beneficiaryId) { this.beneficiaryId = beneficiaryId; }
    public void setPaymentAmount(BigDecimal paymentAmount) { this.paymentAmount = paymentAmount; }
    public void setPaymentType(String paymentType) { this.paymentType = paymentType; }
    public void setIngestionTimestamp(Long ingestionTimestamp) { this.ingestionTimestamp = ingestionTimestamp; }

    @Override
    public String toString() {
        return "Transaction{txnId=" + transactionId
                + ", payer=" + payerId
                + ", beneficiary=" + beneficiaryId
                + ", amount=" + paymentAmount
                + ", type=" + paymentType + '}';
    }
}
