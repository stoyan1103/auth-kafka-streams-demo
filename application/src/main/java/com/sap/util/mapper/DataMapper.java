package com.sap.util.mapper;

import com.sap.authkafkastreams.proto.user.UserView;
import com.sap.authkafkastreams.rest.generated.model.UserResponse;
import com.sap.authkafkastreams.rest.generated.model.UserRole;
import com.sap.user.domain.Role;
import com.sap.user.domain.User;

import static java.time.Instant.ofEpochMilli;
import static java.time.OffsetDateTime.ofInstant;
import static java.time.ZoneOffset.UTC;

public final class DataMapper {

    private DataMapper() {
    }

    public static UserRole toDto(Role role) {
        return UserRole.fromValue(role.name());
    }

    public static UserResponse toDto(User user) {
        return new UserResponse()
            .id(user.id())
            .username(user.username())
            .email(user.email())
            .roles(user.roles()
                .stream()
                .map(DataMapper::toDto)
                .toList()
            )
            .createdAt(ofInstant(user.createdAt(), UTC));
    }

    public static UserResponse toDto(UserView userView) {
        return new UserResponse()
            .id(java.util.UUID.fromString(userView.getUserId()))
            .username(userView.getUsername())
            .email(userView.getEmail())
            .roles(userView.getRolesList()
                .stream()
                .map(UserRole::fromValue)
                .toList()
            )
            .createdAt(ofInstant(ofEpochMilli(userView.getRegisteredAt()), UTC));
    }
}
