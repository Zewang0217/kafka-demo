package org.zewang.kafkademo.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.zewang.kafkademo.model.LogEntry;


/**
 * LogEntryRepository
 * 负责 LogEntry 实体类（对应数据库表）的持久化操作。
 * 继承 JpaRepository 接口，T 为实体类 LogEntry，ID 为主键类型 Long。
 * Spring Data JPA 会自动实现基本的 CRUD 方法，例如 save(), findAll() 等。
 */

@Repository
public interface LogEntryRepository extends JpaRepository<LogEntry, Long> {
    // 我们可以根据需要在这里添加自定义的查询方法，例如：
    // List<LogEntry> findByStatusCode(int statusCode);
    // LogEntry findTopByOrderByTimestampDesc();

}
