package com.example.webfluxplay.dao;

import com.example.webfluxplay.model.SomeEntity;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Service
public final class SomeEntityDao {

  private final R2dbcDao dao;

  public SomeEntityDao() {
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

  private final BiFunction<Row, RowMetadata, SomeEntity> mapper = (row, meta) ->
      new SomeEntity(
          row.get("id", Long.class),
          row.get("svalue", String.class)
      );

  // -----------------------------------------------------------------------
  // CRUD OPERATIONS
  // -----------------------------------------------------------------------

  public Mono<SomeEntity> save(SomeEntity entity) {
    return dao.withConnection(conn ->
        Flux.from(conn.createStatement("INSERT INTO some_entity (svalue) VALUES ($1)")
                .bind("$1", entity.svalue())
                .returnGeneratedValues("id")
                .execute())
            .concatMap(result -> result.map((row, meta) ->
                new SomeEntity(row.get("id", Long.class), entity.svalue())
            ))
    ).next();
  }

  public Flux<SomeEntity> saveAll(List<SomeEntity> entities) {
    if (entities.isEmpty()) {
      return Flux.empty();
    }

    // Use the new Batch API for high-performance inserts
    return dao.batch(
            // 1. Factory: Enable generated keys
            conn -> conn.createStatement("INSERT INTO some_entity (svalue) VALUES ($1)")
                .returnGeneratedValues("id"),

            // 2. Data
            entities,

            // 3. Binder: Map Entity -> Statement
            (stmt, entity) -> stmt.bind("$1", entity.svalue()),

            // 4. Mapper: Extract ID from result
            (row, meta) -> row.get("id", Long.class)
        )
        // 5. Recombine: Match generated IDs back to the original entities
        // (R2DBC guarantees order of results matches order of inputs)
        .zipWithIterable(entities, (id, originalEntity) ->
            new SomeEntity(id, originalEntity.svalue())
        );
  }

  public Mono<SomeEntity> findById(Long id) {
    return dao.select("SELECT id, svalue FROM some_entity WHERE id = $1", mapper, id)
        .next();
  }

  public Flux<SomeEntity> findAll() {
    return dao.select("SELECT id, svalue FROM some_entity", mapper);
  }

  public Mono<Void> deleteById(Long id) {
    return dao.execute("DELETE FROM some_entity WHERE id = $1", id)
        .then();
  }
}