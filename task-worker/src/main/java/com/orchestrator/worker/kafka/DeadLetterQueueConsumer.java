package com.orchestrator.worker.kafka;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.worker.service.WorkerMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    private final WorkerMetricsService metricsService;

    public DeadLetterQueueConsumer(WorkerMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @KafkaListener(
            topics = KafkaTopics.TASK_DLQ,
            groupId = "${spring.kafka.consumer.group-id}-dlq",
            containerFactory = "taskKafkaListenerContainerFactory"
    )
    public void consume(TaskEvent event, Acknowledgment acknowledgment) {
        metricsService.recordDlqProcessed();
        log.error(
                "DLQ recovery visibility - taskId={}, idempotencyKey={}, type={}, retries={}, payload={}",
                event.taskId(),
                event.idempotencyKey(),
                event.taskType(),
                event.retryCount(),
                event.payload()
        );
        acknowledgment.acknowledge();
    }
}
