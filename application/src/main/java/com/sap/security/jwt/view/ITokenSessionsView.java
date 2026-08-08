package com.sap.security.jwt.view;

import java.util.Optional;

import com.sap.authkafkastreams.proto.token_session.TokenSessionView;

public interface ITokenSessionsView {

    Optional<TokenSessionView> findByUserId(String userId);
}
