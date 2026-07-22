package com.orchestrator.worker.kafka;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import com.orchestrator.worker.service.TaskProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TaskQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueConsumer.class);

    private final TaskProcessingService taskProcessingService;

    public TaskQueueConsumer(TaskProcessingService taskProcessingService) {
        this.taskProcessingService = taskProcessingService;
    }

    @KafkaListener(
            topics = KafkaTopics.TASK_QUEUE,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "taskKafkaListenerContainerFactory"
    )
    public void consume(TaskEvent event, Acknowledgment acknowledgment) {
        log.debug("Received task from queue: {}", event.taskId());
        taskProcessingService.process(event);
        acknowledgment.acknowledge();
    }
}
