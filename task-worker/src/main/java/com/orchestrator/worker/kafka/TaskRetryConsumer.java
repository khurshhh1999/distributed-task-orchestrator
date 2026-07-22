package com.orchestrator.worker.kafka;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.worker.service.TaskProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class TaskRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskRetryConsumer.class);

    /**
     * Upper bound on how long a single poll blocks a consumer thread. Longer backoffs
     * are honored by re-queueing the event rather than sleeping the whole duration,
     * so the task is never executed before {@link TaskEvent#scheduledAt()}.
     */
    private static final Duration MAX_INLINE_WAIT = Duration.ofSeconds(5);

    private final TaskProcessingService taskProcessingService;
    private final KafkaTemplate<String, TaskEvent> kafkaTemplate;

    public TaskRetryConsumer(TaskProcessingService taskProcessingService,
                             KafkaTemplate<String, TaskEvent> kafkaTemplate) {
        this.taskProcessingService = taskProcessingService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = KafkaTopics.TASK_RETRY,
            groupId = "${spring.kafka.consumer.group-id}-retry",
            containerFactory = "taskKafkaListenerContainerFactory"
    )
    public void consume(TaskEvent event, Acknowledgment acknowledgment) {
        Instant scheduledAt = event.scheduledAt();
        long remainingMs = scheduledAt == null ? 0L : scheduledAt.toEpochMilli() - Instant.now().toEpochMilli();

        if (remainingMs > MAX_INLINE_WAIT.toMillis()) {
            requeue(event, remainingMs);
            acknowledgment.acknowledge();
            return;
        }

        if (remainingMs > 0) {
            sleep(remainingMs);
        }

        log.debug("Processing retry for task: {} (attempt {})", event.taskId(), event.retryCount());
        taskProcessingService.process(event);
        acknowledgment.acknowledge();
    }

    private void requeue(TaskEvent event, long remainingMs) {
        log.debug("Task {} not due for {}ms, re-queueing to honor backoff schedule", event.taskId(), remainingMs);
        sleep(MAX_INLINE_WAIT.toMillis());
        kafkaTemplate.send(KafkaTopics.TASK_RETRY, event.taskId().toString(), event);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
