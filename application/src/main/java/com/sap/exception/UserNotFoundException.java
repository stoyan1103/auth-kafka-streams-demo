package com.sap.exception;

public class UserNotFoundException extends BaseException{
    public UserNotFoundException(String message) {
        super(message);
    }
}
