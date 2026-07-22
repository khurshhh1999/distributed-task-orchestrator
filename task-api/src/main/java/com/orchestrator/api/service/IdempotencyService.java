package com.orchestrator.api.service;

import com.orchestrator.api.entity.TaskEntity;
import com.orchestrator.api.repository.TaskRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

@Service
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:";
    private static final Duration TTL = Duration.ofDays(7);

    private final StringRedisTemplate redisTemplate;
    private final TaskRepository taskRepository;

    public IdempotencyService(StringRedisTemplate redisTemplate, TaskRepository taskRepository) {
        this.redisTemplate = redisTemplate;
        this.taskRepository = taskRepository;
    }

    public Optional<TaskEntity> findExistingTask(String idempotencyKey) {
        String cachedTaskId = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
        if (cachedTaskId != null) {
            return taskRepository.findById(UUID.fromString(cachedTaskId));
        }
        return taskRepository.findByIdempotencyKey(idempotencyKey);
    }

    public boolean reserveKey(String idempotencyKey) {
        Boolean acquired = redisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + idempotencyKey, "RESERVED", TTL);
        return Boolean.TRUE.equals(acquired);
    }

    public void linkKeyToTask(String idempotencyKey, UUID taskId) {
        redisTemplate.opsForValue().set(KEY_PREFIX + idempotencyKey, taskId.toString(), TTL);
    }

    public void releaseKey(String idempotencyKey) {
        redisTemplate.delete(KEY_PREFIX + idempotencyKey);
    }
}
