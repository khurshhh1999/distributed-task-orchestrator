package com.orchestrator.worker.service;

import com.orchestrator.common.config.RetryPolicy;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.entity.TaskEntity;
import com.orchestrator.worker.repository.TaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class TaskProcessingService {

    private static final Logger log = LoggerFactory.getLogger(TaskProcessingService.class);

    private final TaskRepository taskRepository;
    private final TaskExecutorService taskExecutorService;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final WorkerMetricsService metricsService;
    private final Timer executionTimer;

    public TaskProcessingService(TaskRepository taskRepository,
                                 TaskExecutorService taskExecutorService,
                                 KafkaTemplate<String, TaskEvent> kafkaTemplate,
                                 WorkerMetricsService metricsService,
                                 MeterRegistry meterRegistry) {
        this.taskRepository = taskRepository;
        this.taskExecutorService = taskExecutorService;
        this.kafkaTemplate = kafkaTemplate;
        this.metricsService = metricsService;
        this.executionTimer = Timer.builder("tasks.execution.duration")
                .description("Task execution duration")
                .register(meterRegistry);
    }

    @Transactional
    public void process(TaskEvent event) {
        TaskEntity task = taskRepository.findById(event.taskId())
                .orElseThrow(() -> new IllegalStateException("Task not found: " + event.taskId()));

        if (task.getStatus() == TaskStatus.COMPLETED || task.getStatus() == TaskStatus.DEAD_LETTERED) {
            log.info("Skipping already terminal task {}", task.getId());
            metricsService.recordSkipped();
            return;
        }

        task.setStatus(TaskStatus.RUNNING);
        taskRepository.save(task);

        try {
            executionTimer.record(() -> taskExecutorService.execute(task.getTaskType(), task.getPayload()));
            task.setStatus(TaskStatus.COMPLETED);
            task.setCompletedAt(Instant.now());
            task.setLastError(null);
            task.setNextRetryAt(null);
            taskRepository.save(task);
            metricsService.recordSuccess();
            log.info("Task {} completed successfully", task.getId());
        } catch (TaskExecutionException ex) {
            handleFailure(task, event, ex.getMessage());
        } catch (Exception ex) {
            handleFailure(task, event, ex.getMessage());
        }
    }

    private void handleFailure(TaskEntity task, TaskEvent event, String errorMessage) {
        int nextRetryCount = task.getRetryCount() + 1;
        task.setLastError(errorMessage);
        task.setRetryCount(nextRetryCount);

        if (nextRetryCount > task.getMaxRetries()) {
            sendToDeadLetterQueue(task, event, errorMessage);
            task.setStatus(TaskStatus.DEAD_LETTERED);
            task.setNextRetryAt(null);
            taskRepository.save(task);
            metricsService.recordDeadLettered();
            log.warn("Task {} moved to DLQ after {} retries: {}", task.getId(), nextRetryCount - 1, errorMessage);
            return;
        }

        long backoffMs = RetryPolicy.calculateBackoffMs(nextRetryCount - 1);
        Instant nextRetryAt = Instant.now().plusMillis(backoffMs);
        task.setStatus(TaskStatus.RETRY_SCHEDULED);
        task.setNextRetryAt(nextRetryAt);
        taskRepository.save(task);

        TaskEvent retryEvent = new TaskEvent(
                task.getId(),
                task.getIdempotencyKey(),
                task.getTaskType(),
                task.getPayload(),
                task.getMetadata(),
                nextRetryCount,
                task.getMaxRetries(),
                nextRetryAt
        );

        kafkaTemplate.send(KafkaTopics.TASK_RETRY, task.getId().toString(), retryEvent);
        metricsService.recordRetryScheduled();
        log.info("Task {} scheduled for retry {} at {} (backoff {}ms): {}",
                task.getId(), nextRetryCount, nextRetryAt, backoffMs, errorMessage);
    }

    private void sendToDeadLetterQueue(TaskEntity task, TaskEvent event, String errorMessage) {
        TaskEvent dlqEvent = new TaskEvent(
                task.getId(),
                task.getIdempotencyKey(),
                task.getTaskType(),
                task.getPayload(),
                task.getMetadata(),
                task.getRetryCount(),
                task.getMaxRetries(),
                Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.TASK_DLQ, task.getId().toString(), dlqEvent);
        log.error("DLQ event published for task {} - {}", task.getId(), errorMessage);
    }
}
