package com.sap.web;

import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.RestController;

import com.sap.authkafkastreams.rest.generated.api.UserApi;
import com.sap.authkafkastreams.rest.generated.model.UserListResponse;
import com.sap.authkafkastreams.rest.generated.model.UserResponse;
import com.sap.security.domain.AuthenticatedUser;
import com.sap.user.service.IUserQueryService;
import com.sap.user.service.IUserService;
import com.sap.util.mapper.DataMapper;

import static com.sap.user.domain.Role.ADMIN;
import static java.util.Objects.requireNonNull;
import static org.springframework.http.ResponseEntity.ok;

@RestController
public class UserController implements UserApi {

    private final IUserService userService;
    private final IUserQueryService userQueryService;

    @Autowired
    public UserController(IUserService userService, IUserQueryService userQueryService) {
        this.userService = requireNonNull(userService, "userService must not be null");
        this.userQueryService = requireNonNull(userQueryService, "userQueryService must not be null");
    }

    /*
     * URL-level security only checks that the request is authenticated.
     * This endpoint has resource-level authorization:
     * a user can access only their own profile, unless they are ADMIN.
     */
    @Override
    public ResponseEntity<UserResponse> getUserById(UUID id) {
        AuthenticatedUser currentUser = (AuthenticatedUser) SecurityContextHolder.getContext().getAuthentication().getPrincipal();

        boolean sameUser = currentUser.id().equals(id);
        boolean isAdmin = currentUser.roles().contains(ADMIN);

        if (!sameUser && !isAdmin) {
            throw new AccessDeniedException("User can access only their own profile");
        }

        return ok(userQueryService.getById(id));
    }

    @Override
    public ResponseEntity<UserListResponse> getUsers(Integer page, Integer size) {
        List<UserResponse> users = userService.getUsers().stream()
            .map(DataMapper::toDto)
            .toList();

        UserListResponse response = new UserListResponse()
            .users(users)
            .page(page)
            .size(size)
            .totalElements((long) users.size());

        return ok(response);
    }
}
