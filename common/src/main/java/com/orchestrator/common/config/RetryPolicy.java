package com.orchestrator.common.config;

public final class RetryPolicy {

    public static final int MAX_RETRIES = 5;
    public static final long INITIAL_BACKOFF_MS = 1_000L;
    public static final double BACKOFF_MULTIPLIER = 2.0;
    public static final long MAX_BACKOFF_MS = 60_000L;

    private RetryPolicy() {
    }

    public static long calculateBackoffMs(int retryCount) {
        long backoff = (long) (INITIAL_BACKOFF_MS * Math.pow(BACKOFF_MULTIPLIER, retryCount));
        return Math.min(backoff, MAX_BACKOFF_MS);
    }
}
