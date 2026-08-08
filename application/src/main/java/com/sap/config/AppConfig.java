package com.sap.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;

import com.google.protobuf.Message;
import com.sap.auth.service.IAuthService;
import com.sap.auth.service.impl.AuthService;
import com.sap.kafka.config.KafkaStreamsProperties;
import com.sap.kafka.producer.IKafkaPublisher;
import com.sap.kafka.streams.KafkaStreamsFactory;
import com.sap.notification.stream.NotificationDeliverySimulator;
import com.sap.notification.stream.TokenSessionNotificationRouter;
import com.sap.security.jwt.service.IJwtService;
import com.sap.security.jwt.service.ITokenSessionEventRegistry;
import com.sap.security.jwt.service.impl.TokenSessionEventRegistry;
import com.sap.security.jwt.store.IActiveTokenStore;
import com.sap.security.jwt.store.impl.KafkaAwareActiveTokenStore;
import com.sap.security.jwt.view.ITokenSessionsView;
import com.sap.security.jwt.view.impl.TokenSessionsView;
import com.sap.user.audit.stream.UserAuditStream;
import com.sap.user.repository.IUserRepository;
import com.sap.user.repository.impl.InMemoryUserRepository;
import com.sap.user.service.IUserEventRegistry;
import com.sap.user.service.IUserQueryService;
import com.sap.user.service.IUserService;
import com.sap.user.service.impl.UserEventRegistry;
import com.sap.user.service.impl.UserQueryService;
import com.sap.user.service.impl.UserService;
import com.sap.user.view.IUsersView;
import com.sap.user.view.impl.UsersView;

@Configuration
public class AppConfig {

    @Bean
    public IUserEventRegistry userEventRegistry(IKafkaPublisher<String, Message> publisher) {
        return new UserEventRegistry(publisher);
    }

    @Bean
    public IUserRepository userRepository() {
        return new InMemoryUserRepository();
    }

    @Bean
    public IUserService userService(IUserEventRegistry userEventRegistry, IUserRepository userRepository, PasswordEncoder passwordEncoder) {
        return new UserService(userEventRegistry, userRepository, passwordEncoder);
    }

    @Bean
    public IUserQueryService userQueryService(IUsersView usersView) {
        return new UserQueryService(usersView);
    }

    @Bean
    public ITokenSessionEventRegistry tokenSessionEventRegistry(IKafkaPublisher<String, Message> publisher) {
        return new TokenSessionEventRegistry(publisher);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public ITokenSessionsView tokenSessionsView(KafkaStreamsFactory streamsFactory) {
        return new TokenSessionsView(streamsFactory);
    }

    @Bean
    public IActiveTokenStore activeTokenStore(ITokenSessionsView tokenSessionsView) {
        return new KafkaAwareActiveTokenStore(tokenSessionsView);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public TokenSessionNotificationRouter tokenSessionNotificationRouter(KafkaStreamsFactory streamsFactory) {
        return new TokenSessionNotificationRouter(streamsFactory);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public NotificationDeliverySimulator notificationDeliverySimulator(KafkaStreamsFactory streamsFactory) {
        return new NotificationDeliverySimulator(streamsFactory);
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public UserAuditStream userAuditStream(KafkaStreamsFactory streamsFactory) {
        return new UserAuditStream(streamsFactory);
    }

    @Bean
    public IAuthService authService(
        IJwtService jwtService,
        IActiveTokenStore activeTokenStore,
        ITokenSessionEventRegistry tokenSessionEventRegistry
    ) {
        return new AuthService(jwtService, activeTokenStore, tokenSessionEventRegistry);
    }

    @Bean
    public KafkaStreamsFactory kafkaStreamsFactory(
        @Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
        KafkaStreamsProperties streamsProperties
    ) {
        return new KafkaStreamsFactory(bootstrapServers, streamsProperties.stateDir());
    }

    @Bean(initMethod = "init", destroyMethod = "destroy")
    public IUsersView usersView(KafkaStreamsFactory streamsFactory) {
        return new UsersView(streamsFactory);
    }
}
