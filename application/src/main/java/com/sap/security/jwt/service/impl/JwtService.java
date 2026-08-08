package com.sap.security.jwt.service.impl;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import javax.crypto.SecretKey;

import com.sap.security.config.JwtProperties;
import com.sap.security.jwt.JwtTokenSession;
import com.sap.security.jwt.service.IJwtService;
import com.sap.security.jwt.store.IActiveTokenStore;
import com.sap.user.domain.User;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import static io.jsonwebtoken.io.Decoders.BASE64;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public class JwtService implements IJwtService {

    private static final String USERNAME_CLAIM = "username";
    private static final String ROLES_CLAIM = "roles";

    private final SecretKey signingKey;
    private final Long expirationSeconds;
    private final IActiveTokenStore activeTokenStore;

    public JwtService(JwtProperties jwtProperties, IActiveTokenStore activeTokenStore) {
        byte[] keyBytes = BASE64.decode(jwtProperties.secret());
        this.signingKey = hmacShaKeyFor(keyBytes);
        this.expirationSeconds = jwtProperties.expirationSeconds();
        this.activeTokenStore = requireNonNull(activeTokenStore, "activeTokenStore must not be null");
    }

    @Override
    public JwtTokenSession generateToken(User user) {
        Instant now = now();
        Instant expiresAt = now.plusSeconds(expirationSeconds);
        String tokenId = UUID.randomUUID().toString();

        String token = Jwts.builder()
            .subject(user.id().toString())
            .id(tokenId)
            .claim(USERNAME_CLAIM, user.username())
            .claim(ROLES_CLAIM, user.roles()
                .stream()
                .map(Enum::name)
                .toList()
            )
            .issuedAt(Date.from(now))
            .expiration(Date.from(expiresAt))
            .signWith(signingKey, Jwts.SIG.HS256)
            .compact();

        return new JwtTokenSession(user.id(), tokenId, token, expiresAt);
    }

    @Override
    public Claims parseAndValidateToken(String token) {
        return Jwts.parser()
            .verifyWith(signingKey)
            .build()
            .parseSignedClaims(token)
            .getPayload();
    }

    @Override
    public String extractTokenId(String token) {
        return parseAndValidateToken(token).getId();
    }

    @Override
    public boolean isTokenActive(UUID userId, String tokenId) {
        return activeTokenStore.isActive(userId, tokenId);
    }

    @Override
    public UUID extractUserId(String token) {
        return UUID.fromString(parseAndValidateToken(token).getSubject());
    }
}
