package com.orchestrator.worker.scheduler;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.entity.TaskEntity;
import com.orchestrator.worker.repository.TaskRepository;
import com.orchestrator.worker.service.RetryRecoveryService;
import com.orchestrator.worker.service.WorkerMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Durability safety net for retries. The primary retry path is the Kafka
 * {@code task-retry} topic, but if a broker outage, dropped message, or worker
 * crash leaves a task stranded in {@link TaskStatus#RETRY_SCHEDULED} past its due
 * time, this sweeper reclaims it from the database and re-queues it for processing.
 *
 * <p>A staleness threshold ensures the sweeper only touches retries the live Kafka
 * path failed to run, and the claim itself is guarded by optimistic locking so
 * multiple worker replicas never recover the same task twice.
 */
@Component
public class RetrySweeper {

    private static final Logger log = LoggerFactory.getLogger(RetrySweeper.class);

    private final TaskRepository taskRepository;
    private final RetryRecoveryService retryRecoveryService;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;
    private final WorkerMetricsService metricsService;
    private final Duration staleThreshold;
    private final int batchSize;

    public RetrySweeper(TaskRepository taskRepository,
                        RetryRecoveryService retryRecoveryService,
                        KafkaTemplate<String, TaskEvent> kafkaTemplate,
                        WorkerMetricsService metricsService,
                        @org.springframework.beans.factory.annotation.Value("${orchestrator.retry-sweeper.stale-threshold-ms:30000}") long staleThresholdMs,
                        @org.springframework.beans.factory.annotation.Value("${orchestrator.retry-sweeper.batch-size:100}") int batchSize) {
        this.taskRepository = taskRepository;
        this.retryRecoveryService = retryRecoveryService;
        this.kafkaTemplate = kafkaTemplate;
        this.metricsService = metricsService;
        this.staleThreshold = Duration.ofMillis(staleThresholdMs);
        this.batchSize = batchSize;
    }

    @Scheduled(
            fixedDelayString = "${orchestrator.retry-sweeper.interval-ms:15000}",
            initialDelayString = "${orchestrator.retry-sweeper.initial-delay-ms:20000}"
    )
    public void sweep() {
        Instant cutoff = Instant.now().minus(staleThreshold);
        List<TaskEntity> stuck = taskRepository.findStuckRetries(
                TaskStatus.RETRY_SCHEDULED, cutoff, PageRequest.of(0, batchSize));

        if (stuck.isEmpty()) {
            return;
        }

        int recovered = 0;
        for (TaskEntity task : stuck) {
            try {
                var event = retryRecoveryService.claim(task.getId(), cutoff);
                if (event.isPresent()) {
                    kafkaTemplate.send(KafkaTopics.TASK_QUEUE, task.getId().toString(), event.get());
                    metricsService.recordRetryRecovered();
                    recovered++;
                    log.warn("Recovered stuck retry for task {} (attempt {}), re-queued to {}",
                            task.getId(), task.getRetryCount(), KafkaTopics.TASK_QUEUE);
                }
            } catch (OptimisticLockingFailureException ex) {
                log.debug("Task {} already claimed by another worker, skipping", task.getId());
            } catch (Exception ex) {
                log.error("Failed to recover stuck retry for task {}: {}", task.getId(), ex.getMessage());
            }
        }

        if (recovered > 0) {
            log.info("Retry sweeper recovered {} of {} stuck task(s)", recovered, stuck.size());
        }
    }
}
