package com.orchestrator.worker.service;

public class TaskExecutionException extends RuntimeException {

    public TaskExecutionException(String message) {
        super(message);
    }
}
