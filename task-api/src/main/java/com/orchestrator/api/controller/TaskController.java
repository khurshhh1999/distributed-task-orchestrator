package com.orchestrator.api.controller;

import com.orchestrator.api.service.TaskService;
import com.orchestrator.common.dto.TaskResponse;
import com.orchestrator.common.dto.TaskSubmissionRequest;
import com.orchestrator.common.model.TaskStatus;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    @PostMapping
    public ResponseEntity<TaskResponse> submitTask(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody TaskSubmissionRequest request) {
        TaskResponse response = taskService.submitTask(idempotencyKey, request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @GetMapping("/{taskId}")
    public TaskResponse getTask(@PathVariable UUID taskId) {
        return taskService.getTask(taskId);
    }

    @GetMapping
    public List<TaskResponse> listTasks(@RequestParam(required = false) TaskStatus status) {
        return taskService.listTasks(status);
    }

    @GetMapping("/metrics/summary")
    public Map<String, Long> getStatusSummary() {
        return taskService.getStatusSummary();
    }

    @PostMapping("/{taskId}/replay")
    public TaskResponse replayTask(@PathVariable UUID taskId) {
        return taskService.replayTask(taskId);
    }

    @PostMapping("/dlq/replay")
    public ResponseEntity<Map<String, Object>> replayDeadLettered() {
        List<TaskResponse> replayed = taskService.replayAllDeadLettered();
        return ResponseEntity.accepted().body(Map.of(
                "replayed", replayed.size(),
                "tasks", replayed
        ));
    }
}
