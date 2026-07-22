package com.orchestrator.worker.repository;

import com.orchestrator.common.model.TaskStatus;
import com.orchestrator.worker.entity.TaskEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface TaskRepository extends JpaRepository<TaskEntity, UUID> {

    /**
     * Finds tasks that are still marked for retry but whose scheduled time is at or
     * before {@code cutoff}. The sweeper passes a cutoff in the past (now minus a
     * staleness threshold) so it only recovers retries the Kafka path failed to run.
     */
    @Query("""
            SELECT t FROM TaskEntity t
            WHERE t.status = :status AND t.nextRetryAt <= :cutoff
            ORDER BY t.nextRetryAt ASC
            """)
    List<TaskEntity> findStuckRetries(TaskStatus status, Instant cutoff, Pageable pageable);
}
