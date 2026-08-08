package com.sap.security.jwt;

import java.io.IOException;

import org.jspecify.annotations.NonNull;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import static java.util.Objects.requireNonNull;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;


/*
 * This filter is responsible only for extracting a Bearer JWT from the request
 * and converting it into an Authentication object.
 *
 * It does NOT decide whether the user is allowed to access an endpoint.
 * Authorization is handled later by Spring Security using the authorities
 * stored in the SecurityContext.
 *
 * Flow:
 * 1. Read Authorization: Bearer <token>
 * 2. Create unauthenticated JwtAuthenticationToken
 * 3. Delegate authentication to AuthenticationManager
 * 4. Store successful Authentication in SecurityContextHolder
 */
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final String BEARER_PREFIX = "Bearer ";

    private final AuthenticationManager authenticationManager;
    private final AuthenticationEntryPoint authenticationEntryPoint;

    public JwtAuthenticationFilter(
        AuthenticationManager authenticationManager,
        AuthenticationEntryPoint authenticationEntryPoint
    ) {
        this.authenticationManager = requireNonNull(authenticationManager, "authenticationManager must not be null");
        this.authenticationEntryPoint = requireNonNull(authenticationEntryPoint, "authenticationEntryPoint must not be null");
    }

    @Override
    protected void doFilterInternal(
        HttpServletRequest request,
        @NonNull HttpServletResponse response,
        @NonNull FilterChain filterChain
    ) throws ServletException, IOException {
        String authorizationHeader = request.getHeader(AUTHORIZATION);

        if (authorizationHeader == null || !authorizationHeader.startsWith(BEARER_PREFIX)) {
            filterChain.doFilter(request, response);
            return;
        }

        String token = authorizationHeader.substring(BEARER_PREFIX.length()).trim();

        try {
            Authentication authenticationRequest = new JwtAuthenticationToken(token);
            Authentication authenticationResult = authenticationManager.authenticate(authenticationRequest);

            configureSecurityContext(authenticationResult);

            filterChain.doFilter(request, response);
        } catch (AuthenticationException exception) {
            SecurityContextHolder.clearContext();
            authenticationEntryPoint.commence(request, response, exception);
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getServletPath();

        return path.equals("/api/v1/auth/register")
            || path.equals("/api/v1/auth/login")
            || path.startsWith("/swagger-ui")
            || path.startsWith("/v3/api-docs");
    }

    private static void configureSecurityContext(Authentication authentication) {
        SecurityContext securityContext = SecurityContextHolder.createEmptyContext();
        securityContext.setAuthentication(authentication);
        SecurityContextHolder.setContext(securityContext);
    }
}
