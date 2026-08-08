package com.sap.security.jwt.store.impl;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sap.authkafkastreams.proto.token_session.TokenSessionView;
import com.sap.security.jwt.JwtTokenSession;
import com.sap.security.jwt.store.IActiveTokenStore;
import com.sap.security.jwt.view.ITokenSessionsView;

import static com.sap.authkafkastreams.proto.token_session.TokenSessionStatus.TOKEN_SESSION_STATUS_ACTIVE;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;

public final class KafkaAwareActiveTokenStore implements IActiveTokenStore {

    private final ConcurrentMap<UUID, LocalTokenState> localStates = new ConcurrentHashMap<>();

    private final ITokenSessionsView tokenSessionsView;

    public KafkaAwareActiveTokenStore(ITokenSessionsView tokenSessionsView) {
        this.tokenSessionsView = requireNonNull(tokenSessionsView, "tokenSessionsView must not be null");
    }

    @Override
    public void save(JwtTokenSession session) {
        requireNonNull(session, "session must not be null");

        localStates.put(session.userId(), LocalTokenState.active(session));
    }

    @Override
    public Optional<JwtTokenSession> findByUserId(UUID userId) {
        requireNonNull(userId, "userId must not be null");

        LocalTokenState state = localStates.get(userId);

        if (state == null || state.session().isEmpty()) {
            return Optional.empty();
        }

        JwtTokenSession session = state.session().get();

        if (session.isExpired()) {
            localStates.replace(userId, state, LocalTokenState.inactive(state.tokenId()));

            return Optional.empty();
        }

        return Optional.of(session);
    }

    @Override
    public boolean isActive(UUID userId, String tokenId) {
        requireNonNull(userId, "userId must not be null");
        requireNonNull(tokenId, "tokenId must not be null");

        LocalTokenState localState = localStates.get(userId);

        /*
         * Local state has priority over Kafka because Kafka Streams
         * may not have processed the latest event yet.
         */
        if (localState != null) {
            return localState.tokenId().equals(tokenId) && findByUserId(userId).isPresent();
        }

        return tokenSessionsView.findByUserId(userId.toString())
            .filter(this::isActive)
            .filter(view -> view.getTokenId().equals(tokenId))
            .filter(view -> view.getExpiresAt() > now().toEpochMilli())
            .isPresent();
    }

    @Override
    public void logout(UUID userId, String tokenId) {
        requireNonNull(userId, "userId must not be null");
        requireNonNull(tokenId, "tokenId must not be null");

        localStates.compute(userId, (id, currentState) -> {
            if (currentState != null && !currentState.tokenId().equals(tokenId)) {
                return currentState;
            }

            return LocalTokenState.inactive(tokenId);
        });
    }

    private boolean isActive(TokenSessionView view) {
        return view.getStatus() == TOKEN_SESSION_STATUS_ACTIVE;
    }

    private record LocalTokenState(String tokenId, Optional<JwtTokenSession> session) {

        private static LocalTokenState active(JwtTokenSession session) {
            return new LocalTokenState(session.tokenId(), Optional.of(session));
        }

        private static LocalTokenState inactive(String tokenId) {
            return new LocalTokenState(tokenId, Optional.empty());
        }
    }
}
