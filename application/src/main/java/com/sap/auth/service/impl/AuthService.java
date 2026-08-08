package com.sap.auth.service.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;

import com.sap.auth.service.IAuthService;
import com.sap.security.jwt.JwtTokenSession;
import com.sap.security.jwt.service.IJwtService;
import com.sap.security.jwt.service.ITokenSessionEventRegistry;
import com.sap.security.jwt.store.IActiveTokenStore;
import com.sap.user.domain.User;

import static java.util.Objects.requireNonNull;

public class AuthService implements IAuthService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthService.class);

    private final IJwtService jwtService;
    private final IActiveTokenStore activeTokenStore;
    private final ITokenSessionEventRegistry tokenSessionEventRegistry;

    public AuthService(
        IJwtService jwtService,
        IActiveTokenStore activeTokenStore,
        ITokenSessionEventRegistry tokenSessionEventRegistry
    ) {
        this.jwtService = requireNonNull(jwtService, "jwtService must not be null");
        this.activeTokenStore = requireNonNull(activeTokenStore, "activeTokenStore must not be null");
        this.tokenSessionEventRegistry = requireNonNull(tokenSessionEventRegistry, "tokenSessionEventRegistry must not be null");
    }

    @Override
    public JwtTokenSession getOrCreateActiveTokenSession(User user) {
        JwtTokenSession jwtTokenSession = activeTokenStore.findByUserId(user.id())
            .orElseGet(() -> {
                JwtTokenSession newSession = jwtService.generateToken(user);

                activeTokenStore.save(newSession);

                publishTokenSessionCreatedEvent(newSession);

                LOGGER.info("New token session created for user with id [{}]", user.id());

                return newSession;
            });

        LOGGER.info("Token session for user with id [{}] is active and will be returned", user.id());

        return jwtTokenSession;
    }

    @Override
    public void logout(UUID userId, String tokenId) {
        activeTokenStore.logout(userId, tokenId);

        publishTokenSessionRevokedEvent(userId, tokenId);

        SecurityContextHolder.clearContext();

        LOGGER.info("Token session with id [{}] for user with id [{}] has been deactivated", tokenId, userId);
    }

    private void publishTokenSessionCreatedEvent(JwtTokenSession session) {
        tokenSessionEventRegistry.publishTokenSessionCreatedEvent(session)
            .whenComplete((metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error(
                        "Failed to publish token-session-created event for user with id [{}]",
                        session.userId(), exception
                    );
                    return;
                }

                LOGGER.debug(
                    "Token-session-created event for user with id [{}] published to topic [{}], partition [{}], offset [{}]",
                    session.userId(), metadata.topic(), metadata.partition(), metadata.offset()
                );
            });
    }

    private void publishTokenSessionRevokedEvent(UUID userId, String tokenId) {
        tokenSessionEventRegistry.publishTokenSessionRevokedEvent(userId, tokenId)
            .whenComplete((metadata, exception) -> {
                if (exception != null) {
                    LOGGER.error(
                        "Failed to publish token-session-revoked event for user with id [{}] and token id [{}]",
                        userId, tokenId, exception
                    );
                    return;
                }

                LOGGER.debug(
                    "Token-session-revoked event for user with id [{}] published to topic [{}], partition [{}], offset [{}]",
                    userId, metadata.topic(), metadata.partition(), metadata.offset()
                );
            });
    }
}
