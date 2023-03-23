package com.dimajix.flowman.grpc;

public class RemoteException extends RuntimeException {
    private final String declaredClass;

    public RemoteException(String declaredClass, String message) {
        super(message, null, true, true);

        this.declaredClass = declaredClass;
    }

    public RemoteException(String declaredClass, String message, Throwable cause) {
        super(message, cause, true, true);

        this.declaredClass = declaredClass;
    }

    public String getDeclaredClass() {
        return declaredClass;
    }

    @Override
    public String toString() {
        String s = declaredClass;
        String message = this.getLocalizedMessage();
        return message != null ? s + ": " + message : s;
    }
}
