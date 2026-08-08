package com.sap.security.jwt.service;

import java.util.UUID;

import com.sap.security.jwt.JwtTokenSession;
import com.sap.user.domain.User;

import io.jsonwebtoken.Claims;

public interface IJwtService {

    JwtTokenSession generateToken(User user);

    Claims parseAndValidateToken(String token);

    String extractTokenId(String token);

    boolean isTokenActive(UUID userId, String tokenId);

    UUID extractUserId(String token);
}
