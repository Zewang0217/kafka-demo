package org.zewang.kafkademo.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.zewang.kafkademo.entity.UserEvent;

@Repository
public interface UserEventRepository extends JpaRepository<UserEvent, Long> {

}
