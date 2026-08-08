package com.sap.thread;

public final class ThreadUtils {

    private ThreadUtils() {
    }

    public static void sleepSafely(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted while sleeping", e);
        }
    }
}
