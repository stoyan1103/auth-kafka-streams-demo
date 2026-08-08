package com.sap.kafka.streams;

import java.time.Duration;

public record KafkaStreamsSpec(
    String applicationId,
    Integer numStreamThreads,
    Long commitIntervalMs,
    String autoOffsetReset,
    Duration stopTimeoutDuration
) {

    public KafkaStreamsSpec {
        if (applicationId == null || applicationId.isBlank()) {
            throw new IllegalArgumentException("applicationId must not be null or blank");
        }
    }

    public static KafkaStreamsSpec of(String applicationId) {
        return new KafkaStreamsSpec(applicationId, null, null, null, null);
    }

    public static Builder builder(String applicationId) {
        return new Builder(applicationId);
    }

    public static final class Builder {
        private final String applicationId;
        private Integer numStreamThreads;
        private Long commitIntervalMs;
        private String autoOffsetReset;
        private Duration stopTimeoutDuration;

        private Builder(String applicationId) {
            this.applicationId = applicationId;
        }

        public Builder numStreamThreads(Integer numStreamThreads) {
            this.numStreamThreads = numStreamThreads;
            return this;
        }

        public Builder commitIntervalMs(Long commitIntervalMs) {
            this.commitIntervalMs = commitIntervalMs;
            return this;
        }

        public Builder autoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
            return this;
        }

        public Builder stopTimeoutDuration(Duration stopTimeoutDuration) {
            this.stopTimeoutDuration = stopTimeoutDuration;
            return this;
        }

        public KafkaStreamsSpec build() {
            return new KafkaStreamsSpec(applicationId, numStreamThreads, commitIntervalMs, autoOffsetReset, stopTimeoutDuration);
        }
    }
}
