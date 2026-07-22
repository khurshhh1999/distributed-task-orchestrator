package com.orchestrator.common.kafka;

public final class KafkaTopics {

    public static final String TASK_QUEUE = "task-queue";
    public static final String TASK_RETRY = "task-retry";
    public static final String TASK_DLQ = "task-dlq";

    private KafkaTopics() {
    }
}
