package com.sap.user.service;

import java.util.List;

import com.sap.user.domain.User;

public interface IUserService {

    User register(String username, String email, String password);

    User login(String username, String password);

    User getById(String id);

    List<User> getUsers();
}
