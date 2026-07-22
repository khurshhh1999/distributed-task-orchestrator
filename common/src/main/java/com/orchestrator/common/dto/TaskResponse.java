package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TaskResponse(
        UUID id,
        String idempotencyKey,
        TaskType taskType,
        TaskStatus status,
        int retryCount,
        int maxRetries,
        String payload,
        Map<String, String> metadata,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt
) {
}
