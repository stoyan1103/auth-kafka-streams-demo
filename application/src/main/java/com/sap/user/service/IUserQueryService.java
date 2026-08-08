package com.sap.user.service;

import java.util.UUID;

import com.sap.authkafkastreams.rest.generated.model.UserResponse;

public interface IUserQueryService {

    UserResponse getById(UUID id);
}
