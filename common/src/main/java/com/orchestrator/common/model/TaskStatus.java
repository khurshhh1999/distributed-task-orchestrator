package com.orchestrator.common.model;

public enum TaskStatus {
    PENDING,
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    RETRY_SCHEDULED,
    DEAD_LETTERED
}
