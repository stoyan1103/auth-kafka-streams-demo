package com.sap.security.jwt;

import java.util.Collection;

import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import com.sap.security.domain.AuthenticatedUser;

/*
 * Same Authentication type, two states:
 *
 * Before authentication:
 *   credentials = raw JWT
 *   principal = null
 *   authenticated = false
 *
 * After authentication:
 *   credentials = raw JWT
 *   principal = AuthenticatedUser
 *   authorities = ROLE_USER / ROLE_ADMIN
 *   authenticated = true
 */
public class JwtAuthenticationToken extends AbstractAuthenticationToken {

    private final String token;
    private final AuthenticatedUser principal;

    public JwtAuthenticationToken(String token) {
        super(null);
        this.token = token;
        this.principal = null;
        setAuthenticated(false);
    }

    public JwtAuthenticationToken(
        String token,
        AuthenticatedUser principal,
        Collection<? extends GrantedAuthority> authorities
    ) {
        super(authorities);
        this.token = token;
        this.principal = principal;
        setAuthenticated(true);
    }

    @Override
    public Object getCredentials() {
        return token;
    }

    @Override
    public Object getPrincipal() {
        return principal;
    }
}
