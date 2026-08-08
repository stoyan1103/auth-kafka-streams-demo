package com.sap.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RestController;

import com.sap.auth.service.IAuthService;
import com.sap.authkafkastreams.rest.generated.api.AuthApi;
import com.sap.authkafkastreams.rest.generated.model.LoginUserRequest;
import com.sap.authkafkastreams.rest.generated.model.LoginUserResponse;
import com.sap.authkafkastreams.rest.generated.model.RegisterUserRequest;
import com.sap.authkafkastreams.rest.generated.model.RegisterUserResponse;
import com.sap.security.domain.AuthenticatedUser;
import com.sap.security.jwt.JwtTokenSession;
import com.sap.user.domain.User;
import com.sap.user.service.IUserService;
import com.sap.util.mapper.DataMapper;

import static java.time.OffsetDateTime.ofInstant;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;

@RestController
public class AuthController implements AuthApi {

    private final IUserService userService;
    private final IAuthService authService;

    @Autowired
    public AuthController(IUserService userService, IAuthService authService) {
        this.userService = requireNonNull(userService, "userService must not be null");
        this.authService = requireNonNull(authService, "authService must not be null");
    }

    @Override
    public ResponseEntity<LoginUserResponse> loginUser(LoginUserRequest request) {
        User user = userService.login(request.getUsername(), request.getPassword());

        JwtTokenSession jwtTokenSession = authService.getOrCreateActiveTokenSession(user);

        LoginUserResponse response = new LoginUserResponse()
            .accessToken(jwtTokenSession.token())
            .tokenType("Bearer")
            .expirationInSeconds(jwtTokenSession.remainingValiditySeconds());

        return ok(response);
    }

    @Override
    public ResponseEntity<Void> logoutUser() {
        AuthenticatedUser authenticatedUser = (AuthenticatedUser) SecurityContextHolder
            .getContext()
            .getAuthentication()
            .getPrincipal();

        authService.logout(authenticatedUser.id(), authenticatedUser.tokenId());

        return ResponseEntity.noContent().build();
    }

    @Override
    public ResponseEntity<RegisterUserResponse> registerUser(RegisterUserRequest request) {
        User user = userService.register(request.getUsername(), request.getEmail(), request.getPassword());

        RegisterUserResponse response = new RegisterUserResponse()
            .id(user.id())
            .username(user.username())
            .email(user.email())
            .roles(user.roles().stream().map(DataMapper::toDto).toList())
            .createdAt(ofInstant(user.createdAt(), UTC));

        return status(CREATED).body(response);
    }
}
