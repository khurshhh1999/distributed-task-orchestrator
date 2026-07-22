package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskType;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TaskEvent(
        UUID taskId,
        String idempotencyKey,
        TaskType taskType,
        String payload,
        Map<String, String> metadata,
        int retryCount,
        int maxRetries,
        Instant scheduledAt
) {
}
