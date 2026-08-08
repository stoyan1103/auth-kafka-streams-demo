package com.sap.security.config;

import java.io.IOException;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.authkafkastreams.rest.generated.model.ErrorResponse;
import com.sap.security.jwt.JwtAuthenticationFilter;
import com.sap.security.jwt.JwtAuthenticationProvider;
import com.sap.security.jwt.service.IJwtService;
import com.sap.security.jwt.service.impl.JwtService;
import com.sap.security.jwt.store.IActiveTokenStore;
import com.sap.user.view.IUsersView;

import jakarta.servlet.http.HttpServletResponse;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.security.config.http.SessionCreationPolicy.STATELESS;

/*
 * Stateless API security:
 *
 * /api/v1/auth/register and /api/v1/auth/login are public.
 * All protected endpoints require a valid JWT on every request.
 *
 * No HTTP session is used. The SecurityContext is created from the JWT for
 * the current request only.
 */
@Configuration
public class SecurityConfig {

    @Bean
    public IJwtService jwtService(JwtProperties jwtProperties, IActiveTokenStore activeTokenStore) {
        return new JwtService(jwtProperties, activeTokenStore);
    }

    @Bean
    public JwtAuthenticationProvider jwtAuthenticationProvider(IJwtService jwtService, IUsersView usersView) {
        return new JwtAuthenticationProvider(jwtService, usersView);
    }

    @Bean
    public AuthenticationManager authenticationManager(JwtAuthenticationProvider jwtAuthenticationProvider) {
        return new ProviderManager(jwtAuthenticationProvider);
    }

    @Bean
    public JwtAuthenticationFilter jwtAuthenticationFilter(
        AuthenticationManager authenticationManager,
        AuthenticationEntryPoint authenticationEntryPoint
    ) {
        return new JwtAuthenticationFilter(authenticationManager, authenticationEntryPoint);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SecurityFilterChain securityFilterChain(
        HttpSecurity http,
        JwtAuthenticationFilter jwtAuthenticationFilter,
        AuthenticationEntryPoint authenticationEntryPoint,
        AccessDeniedHandler accessDeniedHandler
    ) throws Exception {
        return http
            .csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(session -> session
                .sessionCreationPolicy(STATELESS)
            )
            .exceptionHandling(exceptions -> exceptions
                .authenticationEntryPoint(authenticationEntryPoint)
                .accessDeniedHandler(accessDeniedHandler)
            )
            .authorizeHttpRequests(authorize -> authorize
                .requestMatchers(POST, "/api/v1/auth/register", "/api/v1/auth/login").permitAll()
                .requestMatchers(POST, "/api/v1/auth/logout").authenticated()

                .requestMatchers("/swagger-ui/**", "/v3/api-docs/**").permitAll()

                .requestMatchers(GET, "/api/v1/users").hasRole("ADMIN")
                .requestMatchers(GET, "/api/v1/users/{id}").authenticated()

                .anyRequest().authenticated()
            )
            .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)
            .build();
    }

    @Bean
    public AuthenticationEntryPoint authenticationEntryPoint(ObjectMapper objectMapper) {
        return (request, response, authException) -> writeErrorResponse(
            objectMapper,
            response,
            UNAUTHORIZED,
            "Unauthorized.",
            List.of("Missing or invalid JWT token.")
        );
    }

    @Bean
    public AccessDeniedHandler accessDeniedHandler(ObjectMapper objectMapper) {
        return (request, response, accessDeniedException) -> writeErrorResponse(
            objectMapper,
            response,
            FORBIDDEN,
            "Forbidden.",
            List.of("You don't have permission to access this resource.")
        );
    }

    private static void writeErrorResponse(
        ObjectMapper objectMapper,
        HttpServletResponse response,
        HttpStatus status,
        String message,
        List<String> details
    ) throws IOException {
        ErrorResponse errorResponse = new ErrorResponse()
            .code(status.value())
            .message(message)
            .details(details);

        response.setStatus(status.value());
        response.setContentType(APPLICATION_JSON_VALUE);

        objectMapper.writeValue(response.getOutputStream(), errorResponse);
    }
}
