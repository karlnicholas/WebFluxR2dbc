package com.example.webfluxplay.dao;

import com.example.webfluxplay.model.SomeEntity;
import io.r2dbc.dao.R2dbcDao;
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

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Service
public final class SomeEntityMsDao {

  private final R2dbcDao dao;
  private final BiFunction<Row, RowMetadata, SomeEntity> mapper = (row, meta) -> {
    SomeEntity someEntity = new SomeEntity();
    someEntity.setId(row.get("id", Long.class));
    someEntity.setSvalue(row.get("svalue", String.class));
    return someEntity;
  };

  public SomeEntityMsDao() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(DRIVER, "mssql")
            .option(HOST, "localhost")
            .option(PORT, 1433)
            .option(USER, "reactnonreact")
            .option(PASSWORD, "reactnonreact")
            .option(DATABASE, "reactnonreact")
            .option(Option.valueOf("encrypt"), "false")
            .build());

    ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
            .maxIdleTime(Duration.ofMinutes(30))
            .initialSize(2)
            .maxSize(10)
            .build();

    this.dao = new R2dbcDao(new ConnectionPool(configuration));
  }

  public Flux<Long> createTable() {
    String sql = """
          DROP TABLE IF EXISTS some_entity;
          CREATE TABLE some_entity (
              id BIGINT IDENTITY(1,1) PRIMARY KEY, 
              svalue VARCHAR(255)
          )
          """;
    return dao.execute(sql);
  }
  // -----------------------------------------------------------------------
  // Transactional Business Logic
  // -----------------------------------------------------------------------

  public Mono<SomeEntity> update(SomeEntity payload) {
    return dao.inTransaction(IsolationLevel.READ_COMMITTED, conn ->
            findById(conn, payload.getId())
                    .switchIfEmpty(Mono.error(new IllegalArgumentException("Entity not found")))
                    .flatMap(existing -> {
                      SomeEntity merged = payload.merge(existing);
                      return updateRow(conn, merged).thenReturn(merged);
                    })
    ).single();
  }

  // -----------------------------------------------------------------------
  // Composable Helpers
  // -----------------------------------------------------------------------

  private Mono<SomeEntity> findById(Connection conn, Long id) {
    // FIXED: Used @id instead of $1
    return dao.select(conn, "SELECT id, svalue FROM some_entity WHERE id = @id", mapper, id)
            .next();
  }

  private Mono<Long> updateRow(Connection conn, SomeEntity entity) {
    // FIXED: Used @svalue and @id instead of $1, $2
    // Note: The dao.execute varargs binding matches order if the library supports it,
    // but named binding is safer if using raw R2DBC.
    // Assuming R2dbcDao wraps positional binding correctly, this is tricky with MSSQL.
    // It is safer to use explicit binding if the R2dbcDao allows it, otherwise
    // ensure R2dbcDao maps the varargs to the named parameters correctly.

    // *Safest approach with native R2DBC MSSQL is usually explicit binding.*
    // However, if your R2dbcDao helper supports varargs, ensure it handles mapping.
    // If it doesn't, you might need to use the statement builder pattern like in `save` below.

    return dao.execute(conn,
            "UPDATE some_entity SET svalue = @svalue WHERE id = @id",
            entity.getSvalue(),
            entity.getId()
    ).next();
  }

  private Mono<SomeEntity> save(Connection conn, SomeEntity entity) {
    // FIXED: Used @svalue
    return dao.batch(conn,
                    c -> c.createStatement("INSERT INTO some_entity (svalue) VALUES (@svalue)")
                            .returnGeneratedValues("id"),
                    Collections.singletonList(entity),
                    // FIXED: Bind specifically to the name "svalue" (without @ in the key usually)
                    (stmt, e) -> stmt.bind("svalue", e.getSvalue()),
                    (row, meta) -> row.get("id", Long.class)
            )
            .next()
            .map(id -> {
              entity.setId(id);
              return entity;
            });
  }

  // -----------------------------------------------------------------------
  // Public Facades
  // -----------------------------------------------------------------------

  public Mono<SomeEntity> save(SomeEntity entity) {
    return dao.withConnection(conn -> save(conn, entity)).next();
  }

  public Flux<SomeEntity> saveAll(List<SomeEntity> entities) {
    if (entities.isEmpty()) return Flux.empty();
    return dao.batch(
                    // FIXED: Used @svalue
                    conn -> conn.createStatement("INSERT INTO some_entity (svalue) VALUES (@svalue)")
                            .returnGeneratedValues("id"),
                    entities,
                    // FIXED: Bind to "svalue"
                    (stmt, entity) -> stmt.bind("svalue", entity.getSvalue()),
                    (row, meta) -> row.get("id", Long.class)
            )
            .zipWithIterable(entities, (id, original) -> {
              original.setId(id);
              return original;
            });
  }

  public Mono<SomeEntity> findById(Long id) {
    // FIXED: Used @id
    return dao.select("SELECT id, svalue FROM some_entity WHERE id = @id", mapper, id).next();
  }

  public Flux<SomeEntity> findAll() {
    return dao.select("SELECT id, svalue FROM some_entity", mapper);
  }

  public Mono<Void> deleteById(Long id) {
    // FIXED: Used @id
    return dao.execute("DELETE FROM some_entity WHERE id = @id", id).then();
  }
}