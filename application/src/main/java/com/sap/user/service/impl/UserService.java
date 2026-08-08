package com.sap.user.service.impl;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;

import com.sap.exception.InvalidCredentialsException;
import com.sap.exception.UserAlreadyExistsException;
import com.sap.exception.UserNotFoundException;
import com.sap.user.domain.User;
import com.sap.user.repository.IUserRepository;
import com.sap.user.service.IUserEventRegistry;
import com.sap.user.service.IUserService;

import static com.sap.user.domain.Role.USER;
import static java.time.Instant.now;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class UserService implements IUserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserService.class);

    private final IUserEventRegistry userEventRegistry;
    private final IUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public UserService(IUserEventRegistry userEventRegistry, IUserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userEventRegistry = requireNonNull(userEventRegistry, "userEventRegistry must not be null");
        this.userRepository = requireNonNull(userRepository, "userRepository must not be null");
        this.passwordEncoder = requireNonNull(passwordEncoder, "passwordEncoder must not be null");
    }

    public User register(String username, String email, String password) {
        if (userRepository.existsByUsername(username)) {
            throw new UserAlreadyExistsException("Username already exists: " + username);
        }
        if (userRepository.existsByEmail(email)) {
            throw new UserAlreadyExistsException("Email already exists: " + email);
        }

        User user = new User(
            randomUUID(),
            username,
            email,
            passwordEncoder.encode(password),
            Set.of(USER),
            now());

        User savedUser = userRepository.save(user);
        LOGGER.info("User with id [{}] has been registered successfully", user.id());

        userEventRegistry.publishUserRegisteredEvent(savedUser)
            .whenComplete((metadata, exception) -> {
                if (exception != null) {
                    // TODO -> Update to use transaction or retry mechanism to ensure the event is published successfully
                    LOGGER.error("Failed to publish user registered event for user with id [{}]", savedUser.id(), exception);
                    return;
                }

                LOGGER.debug("User registered event for user with id [{}] has been published successfully to topic [{}] in partition [{}] at offset [{}]",
                    savedUser.id(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
                );
            });

        return savedUser;
    }

    public User login(String username, String password) {
        User user = userRepository.findByUsername(username)
            .orElseThrow(() -> new InvalidCredentialsException("Invalid username or password"));

        if (!passwordEncoder.matches(password, user.password())) {
            throw new InvalidCredentialsException("Invalid username or password");
        }

        LOGGER.info("User with id [{}] has been logged successfully", user.id());

        return user;
    }

    public User getById(String id) {
        return userRepository.findById(id)
            .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));
    }

    public List<User> getUsers() {
        return userRepository.findAll();
    }
}
