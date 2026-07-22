package com.orchestrator.api.service;

import com.orchestrator.api.entity.TaskEntity;
import com.orchestrator.api.repository.TaskRepository;
import com.orchestrator.common.config.RetryPolicy;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.dto.TaskResponse;
import com.orchestrator.common.dto.TaskSubmissionRequest;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.common.model.TaskStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final IdempotencyService idempotencyService;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final TaskMetricsService metricsService;

    public TaskService(TaskRepository taskRepository,
                       IdempotencyService idempotencyService,
                       KafkaTemplate<String, TaskEvent> kafkaTemplate,
                       TaskMetricsService metricsService) {
        this.taskRepository = taskRepository;
        this.idempotencyService = idempotencyService;
        this.kafkaTemplate = kafkaTemplate;
        this.metricsService = metricsService;
    }

    @Transactional
    public TaskResponse submitTask(String idempotencyKey, TaskSubmissionRequest request) {
        Optional<TaskEntity> existing = idempotencyService.findExistingTask(idempotencyKey);
        if (existing.isPresent()) {
            metricsService.recordIdempotentHit();
            return toResponse(existing.get());
        }

        if (!idempotencyService.reserveKey(idempotencyKey)) {
            existing = taskRepository.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                metricsService.recordIdempotentHit();
                return toResponse(existing.get());
            }
            throw new IllegalStateException("Idempotency key is already being processed");
        }

        try {
            TaskEntity task = new TaskEntity();
            task.setIdempotencyKey(idempotencyKey);
            task.setTaskType(request.taskType());
            task.setPayload(request.payload());
            task.setMetadata(request.metadata() != null ? new HashMap<>(request.metadata()) : new HashMap<>());
            task.setStatus(TaskStatus.QUEUED);
            task.setRetryCount(0);
            task.setMaxRetries(RetryPolicy.MAX_RETRIES);

            TaskEntity saved = taskRepository.save(task);
            idempotencyService.linkKeyToTask(idempotencyKey, saved.getId());

            TaskEvent event = new TaskEvent(
                    saved.getId(),
                    saved.getIdempotencyKey(),
                    saved.getTaskType(),
                    saved.getPayload(),
                    saved.getMetadata(),
                    saved.getRetryCount(),
                    saved.getMaxRetries(),
                    Instant.now()
            );

            kafkaTemplate.send(KafkaTopics.TASK_QUEUE, saved.getId().toString(), event);
            metricsService.recordTaskSubmitted();
            return toResponse(saved);
        } catch (RuntimeException ex) {
            // The DB write rolls back with the transaction, so free the reservation
            // to avoid leaving a dangling key that blocks legitimate resubmissions.
            idempotencyService.releaseKey(idempotencyKey);
            throw ex;
        }
    }

    /**
     * Replays a single dead-lettered task by resetting its retry state and
     * re-publishing it to the task queue for reprocessing.
     */
    @Transactional
    public TaskResponse replayTask(UUID taskId) {
        TaskEntity task = taskRepository.findById(taskId)
                .orElseThrow(() -> new TaskNotFoundException(taskId));
        if (task.getStatus() != TaskStatus.DEAD_LETTERED) {
            throw new IllegalStateException(
                    "Only dead-lettered tasks can be replayed; task " + taskId + " is " + task.getStatus());
        }
        republish(task);
        metricsService.recordReplay();
        return toResponse(task);
    }

    /**
     * Bulk-replays every dead-lettered task. Returns the tasks that were re-queued.
     */
    @Transactional
    public List<TaskResponse> replayAllDeadLettered() {
        List<TaskEntity> deadLettered = taskRepository.findByStatus(TaskStatus.DEAD_LETTERED);
        deadLettered.forEach(this::republish);
        if (!deadLettered.isEmpty()) {
            metricsService.recordReplay(deadLettered.size());
        }
        return deadLettered.stream().map(this::toResponse).toList();
    }

    private void republish(TaskEntity task) {
        task.setStatus(TaskStatus.QUEUED);
        task.setRetryCount(0);
        task.setLastError(null);
        task.setNextRetryAt(null);
        task.setCompletedAt(null);
        TaskEntity saved = taskRepository.save(task);

        TaskEvent event = new TaskEvent(
                saved.getId(),
                saved.getIdempotencyKey(),
                saved.getTaskType(),
                saved.getPayload(),
                saved.getMetadata(),
                saved.getRetryCount(),
                saved.getMaxRetries(),
                Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.TASK_QUEUE, saved.getId().toString(), event);
    }

    @Transactional(readOnly = true)
    public TaskResponse getTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .map(this::toResponse)
                .orElseThrow(() -> new TaskNotFoundException(taskId));
    }

    @Transactional(readOnly = true)
    public List<TaskResponse> listTasks(TaskStatus status) {
        List<TaskEntity> tasks = status != null
                ? taskRepository.findByStatus(status)
                : taskRepository.findAll();
        return tasks.stream().map(this::toResponse).toList();
    }

    @Transactional(readOnly = true)
    public Map<String, Long> getStatusSummary() {
        Map<String, Long> summary = new HashMap<>();
        for (Object[] row : taskRepository.countByStatus()) {
            summary.put(row[0].toString(), (Long) row[1]);
        }
        return summary;
    }

    private TaskResponse toResponse(TaskEntity entity) {
        return new TaskResponse(
                entity.getId(),
                entity.getIdempotencyKey(),
                entity.getTaskType(),
                entity.getStatus(),
                entity.getRetryCount(),
                entity.getMaxRetries(),
                entity.getPayload(),
                entity.getMetadata(),
                entity.getLastError(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getCompletedAt()
        );
    }
}
