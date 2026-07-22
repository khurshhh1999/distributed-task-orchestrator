package com.orchestrator.worker.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
public class WorkerMetricsService {

    private final Counter tasksSucceeded;
    private final Counter tasksRetried;
    private final Counter tasksDeadLettered;
    private final Counter tasksSkipped;
    private final Counter dlqProcessed;
    private final Counter retriesRecovered;

    public WorkerMetricsService(MeterRegistry meterRegistry) {
        this.tasksSucceeded = Counter.builder("tasks.processed.success.total")
                .description("Tasks completed successfully")
                .register(meterRegistry);
        this.tasksRetried = Counter.builder("tasks.processed.retry.scheduled.total")
                .description("Tasks scheduled for retry with backoff")
                .register(meterRegistry);
        this.tasksDeadLettered = Counter.builder("tasks.processed.dlq.total")
                .description("Tasks moved to dead-letter queue")
                .register(meterRegistry);
        this.tasksSkipped = Counter.builder("tasks.processed.skipped.total")
                .description("Tasks skipped due to terminal state")
                .register(meterRegistry);
        this.dlqProcessed = Counter.builder("tasks.dlq.consumed.total")
                .description("DLQ messages consumed for recovery visibility")
                .register(meterRegistry);
        this.retriesRecovered = Counter.builder("tasks.retry.recovered.total")
                .description("Stuck retries recovered and re-queued by the scheduled sweeper")
                .register(meterRegistry);
    }

    public void recordSuccess() {
        tasksSucceeded.increment();
    }

    public void recordRetryScheduled() {
        tasksRetried.increment();
    }

    public void recordDeadLettered() {
        tasksDeadLettered.increment();
    }

    public void recordSkipped() {
        tasksSkipped.increment();
    }

    public void recordDlqProcessed() {
        dlqProcessed.increment();
    }

    public void recordRetryRecovered() {
        retriesRecovered.increment();
    }
}
