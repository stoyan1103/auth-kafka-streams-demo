package com.sap.auth.service;

import java.util.UUID;

import com.sap.security.jwt.JwtTokenSession;
import com.sap.user.domain.User;

public interface IAuthService {

    JwtTokenSession getOrCreateActiveTokenSession(User user);

    void logout(UUID userId, String tokenId);
}
