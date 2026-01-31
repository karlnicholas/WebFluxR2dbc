package com.example.webfluxplay.dao;

import com.example.webfluxplay.model.SomeEntity;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.*;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.H2_DRIVER;
import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

@Service
public final class SomeEntityDao {
  private final ConnectionPool pooledConnection;

  public SomeEntityDao() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, H2_DRIVER)
        .option(PASSWORD, "")
//                .option(URL, "mem:test;DB_CLOSE_DELAY=-1;TRACE_LEVEL_FILE=4")
        .option(URL, "mem:test;DB_CLOSE_DELAY=-1")
//                .option(URL, "tcp://localhost:9092/~/some_entity")
        .option(USER, "sa")
        .build());
    ConnectionPoolConfiguration poolConfiguration = ConnectionPoolConfiguration
        .builder(connectionFactory)
        .initialSize(5)
        .maxSize(10).maxIdleTime(Duration.ofMinutes(5)).build();

    pooledConnection = new ConnectionPool(poolConfiguration);
  }

  public Mono<Long> createTable() {
    return Mono.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("create table if not exists some_entity (id bigint not null generated always as IDENTITY, svalue varchar(255) not null, primary key (id))")
            .execute()).flatMap(result -> Mono.from(result.getRowsUpdated())),
        Connection::close);
  }

  private final BiFunction<Row, RowMetadata, SomeEntity> mapper = (row, rowMetadata) -> {
    SomeEntity someEntity = new SomeEntity();
    someEntity.setId(row.get("id", Long.class));
    someEntity.setSvalue(row.get("svalue", String.class));
    return someEntity;
  };

  public Flux<SomeEntity> findAll() {
    return Flux.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("select * from some_entity")
                .execute())
            .flatMapMany(result -> result.map(mapper)),
        Connection::close);
  }

  public Mono<SomeEntity> save(SomeEntity someEntity) {
    return Mono.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("insert into some_entity(svalue) values ($1)")
                .bind("$1", someEntity.getSvalue())
                .returnGeneratedValues("id")
                .execute())
            .flatMap(result -> Mono.from(result.map((row, rowMetadata) -> {
              someEntity.setId(row.get("id", Long.class));
              return someEntity;
            }))),
        Connection::close);
  }

  public Mono<Long> update(SomeEntity someEntity) {
    return Mono.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("update some_entity set svalue = $1 where id = $2")
                .bind("$1", someEntity.getSvalue())
                .bind("$2", someEntity.getId())
                .execute())
            .flatMap(result -> Mono.from(result.getRowsUpdated())),
        Connection::close);
  }

  public Mono<SomeEntity> findById(Long id) {
    return Mono.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("select * from some_entity where id = $1")
                .bind("$1", id)
                .execute())
            .flatMap(result -> Mono.from(result.map(mapper))),
        Connection::close);
  }

  public Mono<Long> deleteById(Long id) {
    return Mono.usingWhen(pooledConnection.create(), // allocates a connection from the pool
        connection -> Mono.from(connection.createStatement("delete from some_entity where id = $1")
                .bind("$1", id)
                .execute())
            .flatMap(res -> Mono.from(res.getRowsUpdated())),
        Connection::close);
  }

  @SuppressWarnings("java:S3776")
  public Flux<SomeEntity> saveAll(List<SomeEntity> someEntities) {
    if (someEntities.isEmpty()) {
      return Flux.empty();
    }

    return Flux.usingWhen(pooledConnection.create(),
        connection -> {
          Statement s = connection.createStatement("insert into some_entity(svalue) values ($1)")
              .returnGeneratedValues("id");

          for (int i = 0; i < someEntities.size(); i++) {
            // 1. Bind the current entity
            s.bind("$1", someEntities.get(i).getSvalue());

            // 2. ONLY call add() if there is another item coming.
            // This prevents the "dangling empty row" error.
            if (i < someEntities.size() - 1) {
              s.add();
            }
          }

          AtomicInteger indexTracker = new AtomicInteger(0);

          return Flux.from(s.execute())
              .concatMap(result ->
                  result.map((row, rowMetadata) -> {
                    int idx = indexTracker.getAndIncrement();
                    if (idx < someEntities.size()) {
                      SomeEntity entity = someEntities.get(idx);
                      Long id = row.get("id", Long.class);
                      if (id != null) {
                        entity.setId(id);
                      }
                      return entity;
                    } else {
                      return someEntities.get(someEntities.size() - 1);
                    }
                  })
              );
        },
        Connection::close);
  }
}