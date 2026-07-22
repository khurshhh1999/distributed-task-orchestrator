package com.orchestrator.worker.service;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.entity.TaskEntity;
import com.orchestrator.worker.repository.TaskRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class RetryRecoveryService {

    private final TaskRepository taskRepository;

    public RetryRecoveryService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    /**
     * Atomically claims a task that is stuck in {@link TaskStatus#RETRY_SCHEDULED} by
     * flipping it back to {@link TaskStatus#QUEUED}. The write is guarded by the
     * entity's optimistic-lock version, so if another worker claims the same task
     * concurrently the transaction commit fails with an optimistic-lock exception and
     * this claim is discarded.
     *
     * <p>The re-queue Kafka message must only be published <em>after</em> this
     * method returns successfully (i.e. after the transaction commits), which the
     * caller guarantees by publishing the returned event. That prevents a
     * publish-then-rollback double dispatch.
     *
     * @return the event to publish if the claim succeeded, otherwise empty
     */
    @Transactional
    public Optional<TaskEvent> claim(UUID taskId, Instant cutoff) {
        TaskEntity task = taskRepository.findById(taskId).orElse(null);
        if (task == null || task.getStatus() != TaskStatus.RETRY_SCHEDULED) {
            return Optional.empty();
        }
        if (task.getNextRetryAt() == null || task.getNextRetryAt().isAfter(cutoff)) {
            return Optional.empty();
        }

        task.setStatus(TaskStatus.QUEUED);
        task.setNextRetryAt(null);
        taskRepository.save(task);

        return Optional.of(new TaskEvent(
                task.getId(),
                task.getIdempotencyKey(),
                task.getTaskType(),
                task.getPayload(),
                task.getMetadata(),
                task.getRetryCount(),
                task.getMaxRetries(),
                Instant.now()
        ));
    }
}
