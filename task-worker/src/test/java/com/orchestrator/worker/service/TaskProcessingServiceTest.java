package com.orchestrator.worker.service;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.common.model.TaskType;
import com.orchestrator.worker.entity.TaskEntity;
import com.orchestrator.worker.repository.TaskRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskProcessingServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private TaskExecutorService taskExecutorService;

    @Mock
    private KafkaTemplate<String, TaskEvent> kafkaTemplate;

    @Mock
    private WorkerMetricsService metricsService;

    private TaskProcessingService taskProcessingService;

    @BeforeEach
    void setUp() {
        taskProcessingService = new TaskProcessingService(
                taskRepository,
                taskExecutorService,
                kafkaTemplate,
                metricsService,
                new SimpleMeterRegistry()
        );
    }

    @Test
    void completesTaskOnSuccess() {
        UUID taskId = UUID.randomUUID();
        TaskEntity task = buildTask(taskId, 0);
        TaskEvent event = buildEvent(taskId, 0);

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.save(any(TaskEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        taskProcessingService.process(event);

        assertEquals(TaskStatus.COMPLETED, task.getStatus());
        verify(metricsService).recordSuccess();
        verify(kafkaTemplate, never()).send(eq(KafkaTopics.TASK_DLQ), any(), any());
    }

    @Test
    void schedulesRetryOnTransientFailure() {
        UUID taskId = UUID.randomUUID();
        TaskEntity task = buildTask(taskId, 0);
        TaskEvent event = buildEvent(taskId, 0);

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.save(any(TaskEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        doThrow(new TaskExecutionException("boom"))
                .when(taskExecutorService).execute(any(), any());

        taskProcessingService.process(event);

        assertEquals(TaskStatus.RETRY_SCHEDULED, task.getStatus());
        assertEquals(1, task.getRetryCount());
        verify(kafkaTemplate).send(eq(KafkaTopics.TASK_RETRY), eq(taskId.toString()), any(TaskEvent.class));
        verify(metricsService).recordRetryScheduled();
    }

    @Test
    void movesToDlqAfterMaxRetries() {
        UUID taskId = UUID.randomUUID();
        TaskEntity task = buildTask(taskId, 5);
        task.setMaxRetries(5);
        TaskEvent event = buildEvent(taskId, 5);

        when(taskRepository.findById(taskId)).thenReturn(Optional.of(task));
        when(taskRepository.save(any(TaskEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));
        doThrow(new TaskExecutionException("persistent failure"))
                .when(taskExecutorService).execute(any(), any());

        taskProcessingService.process(event);

        assertEquals(TaskStatus.DEAD_LETTERED, task.getStatus());
        ArgumentCaptor<TaskEvent> dlqCaptor = ArgumentCaptor.forClass(TaskEvent.class);
        verify(kafkaTemplate).send(eq(KafkaTopics.TASK_DLQ), eq(taskId.toString()), dlqCaptor.capture());
        assertEquals(taskId, dlqCaptor.getValue().taskId());
        verify(metricsService).recordDeadLettered();
    }

    private TaskEntity buildTask(UUID taskId, int retryCount) {
        TaskEntity task = new TaskEntity();
        task.setId(taskId);
        task.setIdempotencyKey("key-" + taskId);
        task.setTaskType(TaskType.WEBHOOK_CALLBACK);
        task.setStatus(TaskStatus.QUEUED);
        task.setPayload("payload");
        task.setRetryCount(retryCount);
        task.setMaxRetries(5);
        task.setCreatedAt(Instant.now());
        task.setUpdatedAt(Instant.now());
        return task;
    }

    private TaskEvent buildEvent(UUID taskId, int retryCount) {
        return new TaskEvent(
                taskId,
                "key-" + taskId,
                TaskType.WEBHOOK_CALLBACK,
                "payload",
                Map.of(),
                retryCount,
                5,
                Instant.now()
        );
    }
}
