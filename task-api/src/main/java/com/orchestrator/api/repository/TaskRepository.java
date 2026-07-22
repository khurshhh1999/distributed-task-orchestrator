package com.orchestrator.api.repository;

import com.orchestrator.api.entity.TaskEntity;
import com.orchestrator.common.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TaskRepository extends JpaRepository<TaskEntity, UUID> {

    Optional<TaskEntity> findByIdempotencyKey(String idempotencyKey);

    List<TaskEntity> findByStatus(TaskStatus status);

    @Query("SELECT t.status, COUNT(t) FROM TaskEntity t GROUP BY t.status")
    List<Object[]> countByStatus();
}
