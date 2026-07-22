package com.orchestrator.worker.service;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import com.orchestrator.worker.entity.TaskEntity;
import com.orchestrator.worker.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryRecoveryServiceTest {

    @Mock
    private TaskRepository taskRepository;

    private RetryRecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        recoveryService = new RetryRecoveryService(taskRepository);
    }

    @Test
    void claimsStuckRetryAndReturnsEvent() {
        UUID taskId = UUID.randomUUID();
        Instant cutoff = Instant.now();
        TaskEntity task = buildTask(taskId, TaskStatus.RETRY_SCHEDULED, cutoff.minusSeconds(60));

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.save(any(TaskEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        Optional<TaskEvent> event = recoveryService.claim(taskId, cutoff);

        assertTrue(event.isPresent());
        assertEquals(taskId, event.get().taskId());
        assertEquals(TaskStatus.QUEUED, task.getStatus());
        verify(taskRepository).save(task);
    }

    @Test
    void doesNotClaimTaskThatIsNoLongerRetryScheduled() {
        UUID taskId = UUID.randomUUID();
        Instant cutoff = Instant.now();
        TaskEntity task = buildTask(taskId, TaskStatus.RUNNING, cutoff.minusSeconds(60));

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));

        Optional<TaskEvent> event = recoveryService.claim(taskId, cutoff);

        assertTrue(event.isEmpty());
        verify(taskRepository, never()).save(any());
    }

    @Test
    void doesNotClaimRetryThatIsNotYetOverdue() {
        UUID taskId = UUID.randomUUID();
        Instant cutoff = Instant.now();
        TaskEntity task = buildTask(taskId, TaskStatus.RETRY_SCHEDULED, cutoff.plusSeconds(60));

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));

        Optional<TaskEvent> event = recoveryService.claim(taskId, cutoff);

        assertTrue(event.isEmpty());
        verify(taskRepository, never()).save(any());
    }

    private TaskEntity buildTask(UUID taskId, TaskStatus status, Instant nextRetryAt) {
        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setIdempotencyKey("key-" + taskId);
        task.setTaskType(TaskType.DATA_EXPORT);
        task.setStatus(status);
        task.setPayload("payload");
        task.setMetadata(Map.of());
        task.setRetryCount(2);
        task.setMaxRetries(5);
        task.setNextRetryAt(nextRetryAt);
        return task;
    }
}
