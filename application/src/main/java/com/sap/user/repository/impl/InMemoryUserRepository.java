package com.sap.user.repository.impl;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.sap.user.domain.User;
import com.sap.user.repository.IUserRepository;

import static java.util.UUID.fromString;

public class InMemoryUserRepository implements IUserRepository {

    private final ConcurrentMap<UUID, User> usersById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UUID> userIdsByUsername = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UUID> userIdsByEmail = new ConcurrentHashMap<>();

    @Override
    public User save(User user) {
        usersById.put(user.id(), user);
        userIdsByUsername.put(user.username(), user.id());
        userIdsByEmail.put(user.email(), user.id());

        return user;
    }

    @Override
    public Optional<User> findById(String id) {
        return Optional.ofNullable(usersById.get(fromString(id)));
    }

    @Override
    public Optional<User> findByUsername(String username) {
        return Optional.ofNullable(userIdsByUsername.get(username))
            .map(usersById::get);
    }

    @Override
    public List<User> findAll() {
        return List.copyOf(usersById.values());
    }

    @Override
    public boolean existsByUsername(String username) {
        return userIdsByUsername.containsKey(username);
    }

    @Override
    public boolean existsByEmail(String email) {
        return userIdsByEmail.containsKey(email);
    }
}
