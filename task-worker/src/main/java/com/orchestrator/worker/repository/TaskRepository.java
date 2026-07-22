package com.orchestrator.worker.repository;

import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.entity.TaskEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskRepository extends JpaRepository<TaskEntity, UUID> {

    @Query("""
            SELECT t FROM TaskEntity t
            WHERE t.status = :status AND t.nextRetryAt <= :now
            ORDER BY t.nextRetryAt ASC
            """)
    List<TaskEntity> findReadyForRetry(TaskStatus status, Instant now);
}
