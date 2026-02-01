package com.example.webfluxplay.dao;

import com.example.webfluxplay.model.SomeEntity;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Service
public final class SomeEntityDao {

  private final R2dbcDao dao;

  public SomeEntityDao() {
    // (Configuration code remains the same...)
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, H2_DRIVER)
        .option(PASSWORD, "")
        .option(URL, "mem:test;DB_CLOSE_DELAY=-1")
        .option(USER, "sa")
        .build());

    ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
        .maxIdleTime(Duration.ofMinutes(30))
        .initialSize(2)
        .maxSize(10)
        .build();

    this.dao = new R2dbcDao(new ConnectionPool(configuration));
  }

  public Flux<Long> createTable() {
    String sql = "CREATE TABLE IF NOT EXISTS some_entity (id IDENTITY PRIMARY KEY, svalue VARCHAR(255))";
    return dao.execute(sql);
  }

  private final BiFunction<Row, RowMetadata, SomeEntity> mapper = (row, meta) -> {
    SomeEntity someEntity = new SomeEntity();
    someEntity.setId(row.get("id", Long.class));
    someEntity.setSvalue(row.get("svalue", String.class));
    return someEntity;
  };

  // -----------------------------------------------------------------------
  // NEW: Transactional Business Logic
  // -----------------------------------------------------------------------

  /**
   * Atomically updates an entity.
   * Prevents race conditions by locking the read and write in one transaction.
   */
  public Mono<SomeEntity> update(SomeEntity payload) {
    return dao.inTransaction(IsolationLevel.READ_COMMITTED, conn ->
        // 1. Read (using the shared connection)
        findById(conn, payload.getId())
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Entity not found")))
            .flatMap(existing -> {
              // 2. Modify
              SomeEntity merged = payload.merge(existing);
              // 3. Write (using the shared connection)
              return save(conn, merged);
            })
    ).next(); // inTransaction returns Flux, we want Mono
  }

  // -----------------------------------------------------------------------
  // Composable Helpers (Accept Connection)
  // -----------------------------------------------------------------------

  private Mono<SomeEntity> findById(Connection conn, Long id) {
    return dao.select(conn, "SELECT id, svalue FROM some_entity WHERE id = $1", mapper, id)
        .next();
  }

  private Mono<SomeEntity> save(Connection conn, SomeEntity entity) {
    // We use 'batch' even for a single item because it allows
    // us to use the generated keys factory logic cleanly.
    return dao.batch(conn,
            c -> c.createStatement("INSERT INTO some_entity (svalue) VALUES ($1)").returnGeneratedValues("id"),
            Collections.singletonList(entity),
            (stmt, e) -> stmt.bind("$1", e.getSvalue()),
            (row, meta) -> row.get("id", Long.class)
        )
        .next()
        .map(id -> {
          entity.setId(id);
          return entity;
        });
  }

  // -----------------------------------------------------------------------
  // Public Facades (Manage Connection Lifecycle)
  // -----------------------------------------------------------------------

  public Mono<SomeEntity> save(SomeEntity entity) {
    return dao.withConnection(conn -> save(conn, entity)).next();
  }

  public Flux<SomeEntity> saveAll(List<SomeEntity> entities) {
    if (entities.isEmpty()) return Flux.empty();

    // This can also be wrapped in dao.inTransaction if "All or Nothing" is required
    return dao.batch(
            conn -> conn.createStatement("INSERT INTO some_entity (svalue) VALUES ($1)").returnGeneratedValues("id"),
            entities,
            (stmt, entity) -> stmt.bind("$1", entity.getSvalue()),
            (row, meta) -> row.get("id", Long.class)
        )
        .zipWithIterable(entities, (id, original) -> {
          original.setId(id);
          return original;
        });
  }

  public Mono<SomeEntity> findById(Long id) {
    return dao.select("SELECT id, svalue FROM some_entity WHERE id = $1", mapper, id).next();
  }

  public Flux<SomeEntity> findAll() {
    return dao.select("SELECT id, svalue FROM some_entity", mapper);
  }

  public Mono<Void> deleteById(Long id) {
    return dao.execute("DELETE FROM some_entity WHERE id = $1", id).then();
  }
}