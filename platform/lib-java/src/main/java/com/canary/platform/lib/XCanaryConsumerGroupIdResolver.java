package com.canary.platform.lib;

/**
 * Resolves a per-subset Kafka consumer group ID by appending the version
 * suffix to a base group ID. Used by services to ensure stable + canary
 * pods join different consumer groups.
 */
public class XCanaryConsumerGroupIdResolver {

    private final String version;

    public XCanaryConsumerGroupIdResolver(String version) {
        this.version = (version == null || version.isBlank()) ? "stable" : version.trim();
    }

    public String resolve(String baseGroupId) {
        if (baseGroupId == null || baseGroupId.isBlank()) {
            throw new IllegalArgumentException("baseGroupId must not be null or blank");
        }
        return baseGroupId + "-" + version;
    }

    public String version() {
        return version;
    }
}
