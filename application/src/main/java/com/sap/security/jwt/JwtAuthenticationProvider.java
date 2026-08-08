package com.sap.security.jwt;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.sap.authkafkastreams.proto.user.UserView;
import com.sap.security.domain.AuthenticatedUser;
import com.sap.security.jwt.service.IJwtService;
import com.sap.user.domain.Role;
import com.sap.user.view.IUsersView;

import io.jsonwebtoken.JwtException;
import static java.util.Objects.requireNonNull;

/*
 * AuthenticationProvider contains the actual JWT authentication logic.
 *
 * It validates the token, extracts the user id, loads the current user from
 * the application, maps roles to Spring Security authorities, and returns an
 * authenticated JwtAuthenticationToken.
 */
public class JwtAuthenticationProvider implements AuthenticationProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtAuthenticationProvider.class);

    private static final String ROLE_PREFIX = "ROLE_";

    private final IJwtService jwtService;
    private final IUsersView usersView;

    public JwtAuthenticationProvider(IJwtService jwtService, IUsersView usersView) {
        this.jwtService = requireNonNull(jwtService, "jwtService must not be null");
        this.usersView = requireNonNull(usersView, "usersView must not be null");
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String token = (String) authentication.getCredentials();

        try {
            UUID userId = jwtService.extractUserId(token);
            String tokenId = jwtService.extractTokenId(token);

            validateTokenIsActive(userId, tokenId);

            UserView user = usersView.findById(userId.toString())
                .orElseThrow(() -> {
                    LOGGER.warn(
                        "User with id [{}] from JWT was not found in users view",
                        userId
                    );

                    return new BadCredentialsException(
                        "User not found for JWT token."
                    );
                });

            Set<Role> roles = user.getRolesList()
                .stream()
                .map(Role::valueOf)
                .collect(Collectors.toUnmodifiableSet());

            List<SimpleGrantedAuthority> authorities = roles
                .stream()
                .map(role -> new SimpleGrantedAuthority(ROLE_PREFIX + role.name()))
                .toList();

            AuthenticatedUser principal = new AuthenticatedUser(userId, user.getUsername(), roles, tokenId);

            return new JwtAuthenticationToken(token, principal, authorities);
        } catch (JwtException | IllegalArgumentException exception) {
            LOGGER.error("JWT Token is invalid", exception);
            throw new BadCredentialsException("Invalid JWT token.");
        }
    }

    private void validateTokenIsActive(UUID userId, String tokenId) {
        if (!jwtService.isTokenActive(userId, tokenId)) {
            LOGGER.warn("JWT Token with id [{}] for user with id [{}] is not active", tokenId, userId);
            throw new BadCredentialsException("JWT token is not active.");
        }
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return JwtAuthenticationToken.class.isAssignableFrom(authentication);
    }
}
