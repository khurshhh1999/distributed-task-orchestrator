package com.orchestrator.api.service;

import com.orchestrator.api.entity.TaskEntity;
import com.orchestrator.api.repository.TaskRepository;
import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.dto.TaskSubmissionRequest;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private KafkaTemplate<String, TaskEvent> kafkaTemplate;

    @Mock
    private TaskMetricsService metricsService;

    private TaskService taskService;

    @BeforeEach
    void setUp() {
        taskService = new TaskService(taskRepository, idempotencyService, kafkaTemplate, metricsService);
    }

    @Test
    void returnsExistingTaskForDuplicateIdempotencyKey() {
        TaskEntity existing = new TaskEntity();
        existing.setId(UUID.randomUUID());
        existing.setIdempotencyKey("dup-key");
        existing.setTaskType(TaskType.EMAIL_NOTIFICATION);
        existing.setStatus(TaskStatus.COMPLETED);
        existing.setPayload("test");
        existing.setRetryCount(0);
        existing.setMaxRetries(5);

        when(idempotencyService.findExistingTask("dup-key")).thenReturn(Optional.of(existing));

        TaskSubmissionRequest request = new TaskSubmissionRequest(
                TaskType.EMAIL_NOTIFICATION, "test", Map.of()
        );

        var response = taskService.submitTask("dup-key", request);

        assertEquals(existing.getId(), response.id());
        assertEquals(TaskStatus.COMPLETED, response.status());
        verify(metricsService).recordIdempotentHit();
        verify(taskRepository, never()).save(any());
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void submitsNewTaskAndPublishesToKafka() {
        when(idempotencyService.findExistingTask("new-key")).thenReturn(Optional.empty());
        when(idempotencyService.reserveKey("new-key")).thenReturn(true);

        TaskEntity saved = new TaskEntity();
        UUID taskId = UUID.randomUUID();
        saved.setId(taskId);
        saved.setIdempotencyKey("new-key");
        saved.setTaskType(TaskType.DATA_EXPORT);
        saved.setStatus(TaskStatus.QUEUED);
        saved.setPayload("export-data");
        saved.setRetryCount(0);
        saved.setMaxRetries(5);

        when(taskRepository.save(any(TaskEntity.class))).thenReturn(saved);

        TaskSubmissionRequest request = new TaskSubmissionRequest(
                TaskType.DATA_EXPORT, "export-data", Map.of("region", "us-east-1")
        );

        var response = taskService.submitTask("new-key", request);

        assertEquals(taskId, response.id());
        assertEquals(TaskStatus.QUEUED, response.status());

        ArgumentCaptor<TaskEvent> eventCaptor = ArgumentCaptor.forClass(TaskEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.TASK_QUEUE), eq(taskId.toString()), eventCaptor.capture());
        assertEquals(taskId, eventCaptor.getValue().taskId());
        verify(metricsService).recordTaskSubmitted();
    }

    @Test
    void replaysDeadLetteredTaskBackOntoQueue() {
        UUID taskId = UUID.randomUUID();
        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setIdempotencyKey("dlq-key");
        task.setTaskType(TaskType.WEBHOOK_CALLBACK);
        task.setStatus(TaskStatus.DEAD_LETTERED);
        task.setPayload("payload");
        task.setRetryCount(6);
        task.setMaxRetries(5);
        task.setLastError("permanent failure");

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.save(any(TaskEntity.class))).thenAnswer(inv -> inv.getArgument(0));

        var response = taskService.replayTask(taskId);

        assertEquals(TaskStatus.QUEUED, response.status());
        assertEquals(0, response.retryCount());
        verify(kafkaTemplate).send(eq(KafkaTopics.TASK_QUEUE), eq(taskId.toString()), any(TaskEvent.class));
        verify(metricsService).recordReplay();
    }

    @Test
    void rejectsReplayOfNonDeadLetteredTask() {
        UUID taskId = UUID.randomUUID();
        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setStatus(TaskStatus.COMPLETED);

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));

        assertThrows(IllegalStateException.class, () -> taskService.replayTask(taskId));
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
