package com.sap.web;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.sap.authkafkastreams.rest.generated.model.ErrorResponse;
import com.sap.exception.InvalidCredentialsException;
import com.sap.exception.StoreUnavailableException;
import com.sap.exception.UserAlreadyExistsException;
import com.sap.exception.UserNotFoundException;

import static java.lang.String.format;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private static final String INTERNAL_ERROR_MESSAGE = "An unexpected error occurred.";
    private static final String NOT_NULL = "NotNull";
    private static final String REQUEST_VALIDATION_FAILED = "Request validation failed.";
    private static final String NO_PERMISSION_MESSAGE = "You don't have permission to access this resource.";
    private static final String INVALID_TYPE_MESSAGE = "Invalid request parameter.";

    @ExceptionHandler(UserAlreadyExistsException.class)
    public ResponseEntity<ErrorResponse> handleUserAlreadyExistsException(UserAlreadyExistsException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(CONFLICT, exception.getMessage(), null);
    }

    @ExceptionHandler(InvalidCredentialsException.class)
    public ResponseEntity<ErrorResponse> handleInvalidCredentialsException(InvalidCredentialsException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(UNAUTHORIZED, exception.getMessage(), null);
    }

    @ExceptionHandler(UserNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleUserNotFoundException(UserNotFoundException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(NOT_FOUND, exception.getMessage(), null);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDeniedException(AccessDeniedException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(FORBIDDEN, "Forbidden.", List.of(NO_PERMISSION_MESSAGE));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(BAD_REQUEST, exception.getMessage(), null);
    }

    @ExceptionHandler(StoreUnavailableException.class)
    public ResponseEntity<ErrorResponse> handleStoreUnavailableException(StoreUnavailableException exception) {
        LOGGER.warn(exception.getMessage());
        return buildErrorResponse(SERVICE_UNAVAILABLE, exception.getMessage(), null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentNotValidException(MethodArgumentNotValidException exception) {
        LOGGER.warn("Validation failed: {}", exception.getMessage(), exception);

        List<String> errorDetails = exception.getBindingResult()
            .getFieldErrors()
            .stream()
            .map(GlobalExceptionHandler::toValidationMessage)
            .toList();

        String message = errorDetails.isEmpty()
            ? REQUEST_VALIDATION_FAILED
            : errorDetails.getFirst();

        return buildErrorResponse(BAD_REQUEST, message, errorDetails);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleMethodArgumentTypeMismatchException(MethodArgumentTypeMismatchException exception) {
        String parameterName = exception.getName();
        Object rejectedValue = exception.getValue();
        Class<?> requiredType = exception.getRequiredType();

        String requiredTypeName = requiredType == null
            ? "required type"
            : requiredType.getSimpleName();

        String detail = "Parameter '%s' must be a valid %s. Rejected value: '%s'"
            .formatted(parameterName, requiredTypeName, rejectedValue);

        LOGGER.warn("Type mismatch: {}", detail, exception);

        return buildErrorResponse(BAD_REQUEST, INVALID_TYPE_MESSAGE, List.of(detail));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception exception) {
        LOGGER.error(INTERNAL_ERROR_MESSAGE, exception);
        return buildErrorResponse(INTERNAL_SERVER_ERROR, INTERNAL_ERROR_MESSAGE, null);
    }

    private static ResponseEntity<ErrorResponse> buildErrorResponse(HttpStatus status, String message, List<String> errorDetails) {
        return ResponseEntity.status(status).body(new ErrorResponse().code(status.value()).message(message).details(errorDetails));
    }

    private static String toValidationMessage(FieldError fieldError) {
        String field = fieldError.getField();

        if (NOT_NULL.equals(fieldError.getCode())) {
            return format("%s is required", field);
        }

        String defaultMessage = fieldError.getDefaultMessage();

        if (defaultMessage == null || defaultMessage.isBlank()) {
            return format("%s is invalid", field);
        }

        return format("%s %s", field, defaultMessage);
    }
}
