package com.sap.user.domain;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record User(UUID id, String username, String email, String password, Set<Role> roles, Instant createdAt) {
}
