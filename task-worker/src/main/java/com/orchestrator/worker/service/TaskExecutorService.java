package com.orchestrator.worker.service;

import com.orchestrator.common.model.TaskType;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class TaskExecutorService {

    public void execute(TaskType taskType, String payload) {
        if (payload != null && payload.contains("force-fail")) {
            throw new TaskExecutionException("Simulated failure for payload: " + payload);
        }

        if (payload != null && payload.contains("transient-fail")) {
            if (ThreadLocalRandom.current().nextDouble() < 0.7) {
                throw new TaskExecutionException("Transient failure - will retry");
            }
        }

        switch (taskType) {
            case EMAIL_NOTIFICATION -> simulateWork(50);
            case DATA_EXPORT -> simulateWork(120);
            case WEBHOOK_CALLBACK -> simulateWork(80);
            case REPORT_GENERATION -> simulateWork(200);
            default -> simulateWork(30);
        }
    }

    private void simulateWork(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new TaskExecutionException("Task interrupted");
        }
    }
}
