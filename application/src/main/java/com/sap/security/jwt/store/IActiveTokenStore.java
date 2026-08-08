package com.sap.security.jwt.store;

import java.util.Optional;
import java.util.UUID;

import com.sap.security.jwt.JwtTokenSession;

public interface IActiveTokenStore {

    void save(JwtTokenSession session);

    Optional<JwtTokenSession> findByUserId(UUID userId);

    boolean isActive(UUID userId, String tokenId);

     void logout(UUID userId, String tokenId);
}
