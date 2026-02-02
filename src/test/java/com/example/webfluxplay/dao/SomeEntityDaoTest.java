package com.example.webfluxplay.dao;

import com.example.webfluxplay.model.SomeEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

@SpringBootTest
class SomeEntityDaoTest {

  @Autowired
  private SomeEntityDao dao;

  @BeforeEach
  void setUp() {
    // Since the DB is persistent in memory (DB_CLOSE_DELAY=-1),
    // we must clean up and ensure the table exists before every test.
    // The ApplicationReadyEvent in main might have run, but we want a clean slate.

    dao.createTable().blockLast();

    // Truncate to ensure isolation between tests
    // We use the raw dao execute capability exposed via a helper or just rely on delete logic
    // For simplicity here, we can delete all items one by one or drop/create.
    // Let's use findAll + deleteById to be safe with available API.
    dao.findAll()
        .flatMap(e -> dao.deleteById(e.getId()))
        .blockLast();
  }

  @Test
  void save_shouldPersistEntity_andReturnId() {
    SomeEntity entity = new SomeEntity();
    entity.setSvalue("test-value");

    StepVerifier.create(dao.save(entity))
        .assertNext(saved -> {
          if (saved.getId() == null) throw new AssertionError("ID should not be null");
          if (!"test-value".equals(saved.getSvalue())) throw new AssertionError("Value mismatch");
        })
        .verifyComplete();
  }

  @Test
  void findById_shouldReturnEntity_whenExists() {
    SomeEntity entity = new SomeEntity();
    entity.setSvalue("find-me");
    SomeEntity saved = dao.save(entity).block();

    StepVerifier.create(dao.findById(saved.getId()))
        .assertNext(found -> {
          if (!found.getId().equals(saved.getId())) throw new AssertionError("ID mismatch");
          if (!"find-me".equals(found.getSvalue())) throw new AssertionError("Value mismatch");
        })
        .verifyComplete();
  }

  @Test
  void update_shouldModifyEntity_inTransaction() {
    // 1. Create
    SomeEntity original = new SomeEntity();
    original.setSvalue("original");
    SomeEntity saved = dao.save(original).block();

    // 2. Prepare Update Payload
    SomeEntity updatePayload = new SomeEntity();
    updatePayload.setId(saved.getId());
    updatePayload.setSvalue("updated");

    // 3. Update & Verify
    StepVerifier.create(dao.update(updatePayload))
        .assertNext(updated -> {
          if (!"updated".equals(updated.getSvalue())) throw new AssertionError("Update failed");
        })
        .verifyComplete();

    // 4. Verify in DB
    StepVerifier.create(dao.findById(saved.getId()))
        .assertNext(found -> {
          if (!"updated".equals(found.getSvalue())) throw new AssertionError("DB verify failed");
        })
        .verifyComplete();
  }

  @Test
  void saveAll_shouldPersistBatch() {
    SomeEntity e1 = new SomeEntity(); e1.setSvalue("batch-1");
    SomeEntity e2 = new SomeEntity(); e2.setSvalue("batch-2");
    List<SomeEntity> list = Arrays.asList(e1, e2);

    StepVerifier.create(dao.saveAll(list))
        .expectNextCount(2)
        .verifyComplete();

    StepVerifier.create(dao.findAll())
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  void deleteById_shouldRemoveEntity() {
    SomeEntity entity = new SomeEntity();
    entity.setSvalue("delete-me");
    SomeEntity saved = dao.save(entity).block();

    StepVerifier.create(dao.deleteById(saved.getId()))
        .verifyComplete();

    StepVerifier.create(dao.findById(saved.getId()))
        .verifyComplete(); // Expect empty
  }
}