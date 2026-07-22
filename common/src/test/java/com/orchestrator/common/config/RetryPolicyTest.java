package com.orchestrator.common.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryPolicyTest {

    @Test
    void calculatesExponentialBackoff() {
        assertEquals(1_000L, RetryPolicy.calculateBackoffMs(0));
        assertEquals(2_000L, RetryPolicy.calculateBackoffMs(1));
        assertEquals(4_000L, RetryPolicy.calculateBackoffMs(2));
        assertEquals(8_000L, RetryPolicy.calculateBackoffMs(3));
        assertEquals(16_000L, RetryPolicy.calculateBackoffMs(4));
    }

    @Test
    void capsBackoffAtMaximum() {
        assertEquals(60_000L, RetryPolicy.calculateBackoffMs(10));
    }
}
