package com.snapbase;

/**
 * Exception thrown by Snapbase operations.
 * 
 * This exception wraps errors that occur in the native Rust code
 * and provides meaningful error messages to Java applications.
 */
public class SnapbaseException extends Exception {
    
    /**
     * Create a new SnapbaseException with a message.
     * 
     * @param message Error message describing what went wrong
     */
    public SnapbaseException(String message) {
        super(message);
    }
    
    /**
     * Create a new SnapbaseException with a message and cause.
     * 
     * @param message Error message describing what went wrong
     * @param cause The underlying cause of the error
     */
    public SnapbaseException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Create a new SnapbaseException with a cause.
     * 
     * @param cause The underlying cause of the error
     */
    public SnapbaseException(Throwable cause) {
        super(cause);
    }
}