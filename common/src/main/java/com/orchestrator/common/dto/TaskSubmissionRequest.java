package com.orchestrator.common.dto;

import com.orchestrator.common.model.TaskType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Map;

public record TaskSubmissionRequest(
        @NotNull TaskType taskType,
        @NotBlank String payload,
        Map<String, String> metadata
) {
}
