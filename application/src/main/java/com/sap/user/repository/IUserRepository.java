package com.sap.user.repository;

import java.util.List;
import java.util.Optional;

import com.sap.user.domain.User;

public interface IUserRepository {

    User save(User user);

    Optional<User> findById(String id);

    Optional<User> findByUsername(String username);

    List<User> findAll();

    boolean existsByUsername(String username);

    boolean existsByEmail(String email);
}
