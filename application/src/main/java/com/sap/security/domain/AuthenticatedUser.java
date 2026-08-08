package com.sap.security.domain;

import java.util.Set;
import java.util.UUID;

import com.sap.user.domain.Role;

public record AuthenticatedUser(UUID id, String username, Set<Role> roles, String tokenId) {
}
