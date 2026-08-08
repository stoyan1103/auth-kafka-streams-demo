package com.sap.security.jwt;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static java.time.Instant.now;

public record JwtTokenSession(UUID userId, String tokenId, String token, Instant expiresAt) {

    public boolean isExpired() {
        return now().isAfter(expiresAt);
    }

    public long remainingValiditySeconds() {
        return Math.max(0, Duration.between(now(), expiresAt).toSeconds());
    }
}
