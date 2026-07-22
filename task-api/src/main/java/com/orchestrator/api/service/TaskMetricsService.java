package com.orchestrator.api.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

@Service
public class TaskMetricsService {

    private final Counter tasksSubmitted;
    private final Counter idempotentHits;
    private final Counter tasksReplayed;

    public TaskMetricsService(MeterRegistry meterRegistry) {
        this.tasksSubmitted = Counter.builder("tasks.submitted.total")
                .description("Total number of tasks submitted to the orchestrator")
                .register(meterRegistry);
        this.idempotentHits = Counter.builder("tasks.idempotent.hits.total")
                .description("Duplicate submissions prevented by idempotency keys")
                .register(meterRegistry);
        this.tasksReplayed = Counter.builder("tasks.dlq.replayed.total")
                .description("Dead-lettered tasks replayed back onto the task queue")
                .register(meterRegistry);
    }

    public void recordTaskSubmitted() {
        tasksSubmitted.increment();
    }

    public void recordIdempotentHit() {
        idempotentHits.increment();
    }

    public void recordReplay() {
        tasksReplayed.increment();
    }

    public void recordReplay(int count) {
        tasksReplayed.increment(count);
    }
}
