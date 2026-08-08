package com.sap.user.service.impl;

import java.util.UUID;

import com.sap.authkafkastreams.rest.generated.model.UserResponse;
import com.sap.exception.UserNotFoundException;
import com.sap.user.service.IUserQueryService;
import com.sap.user.view.IUsersView;

import static com.sap.util.mapper.DataMapper.toDto;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UserQueryService implements IUserQueryService {

    private final IUsersView usersView;

    public UserQueryService(IUsersView usersView) {
        this.usersView = requireNonNull(usersView, "usersView must not be null");
    }

    @Override
    public UserResponse getById(UUID id) {
        requireNonNull(id, "id must not be null");

        return toDto(usersView.findById(id.toString())
            .orElseThrow(() -> new UserNotFoundException(format("User with id %s not found", id))));
    }
}
