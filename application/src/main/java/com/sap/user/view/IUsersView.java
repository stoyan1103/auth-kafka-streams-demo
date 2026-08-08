package com.sap.user.view;

import java.util.Optional;

import com.sap.authkafkastreams.proto.user.UserView;

public interface IUsersView {

    Optional<UserView> findById(String userId);
}
