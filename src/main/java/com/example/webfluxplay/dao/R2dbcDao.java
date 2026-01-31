package com.example.webfluxplay.dao;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A minimalist R2DBC Data Access Object.
 */
public class R2dbcDao {

  private final ConnectionFactory connectionFactory;

  public R2dbcDao(ConnectionFactory connectionFactory) {
    if (connectionFactory == null) throw new IllegalArgumentException("ConnectionFactory must not be null");
    this.connectionFactory = connectionFactory;
  }

  public <T> Flux<T> withConnection(Function<Connection, ? extends Publisher<T>> action) {
    return Flux.usingWhen(
        connectionFactory.create(),
        connection -> Flux.from(action.apply(connection)),
        Connection::close
    );
  }

  public <T> Flux<T> inTransaction(Function<Connection, ? extends Publisher<T>> action) {
    return withConnection(conn ->
        Mono.from(conn.beginTransaction())
            .thenMany(Flux.from(action.apply(conn)))
            .concatWith(Flux.defer(() -> Mono.from(conn.commitTransaction()).then(Mono.empty())))
            .onErrorResume(e -> Mono.from(conn.rollbackTransaction()).then(Mono.error(e)))
    );
  }

  public Flux<Long> execute(String sql, Object... parameters) {
    return withConnection(conn -> {
      Statement statement = conn.createStatement(sql);
      bindParameters(statement, parameters);
      return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated);
    });
  }

  public <T> Flux<T> select(String sql, BiFunction<Row, RowMetadata, T> mapper, Object... parameters) {
    return withConnection(conn -> {
      Statement statement = conn.createStatement(sql);
      bindParameters(statement, parameters);
      return Flux.from(statement.execute()).concatMap(result -> result.map(mapper));
    });
  }

  // -------------------------------------------------------------------------
  // High-Performance Batch Support
  // -------------------------------------------------------------------------

  /**
   * Executes a batch update/insert returning the number of affected rows.
   * Used for bulk operations where generated keys are not needed.
   */
  public <T> Flux<Long> batch(String sql, Iterable<T> items, BiConsumer<Statement, T> binder) {
    return withConnection(conn ->
        Flux.from(prepareBatch(conn, c -> c.createStatement(sql), items, binder).execute())
            .concatMap(Result::getRowsUpdated)
    );
  }

  /**
   * Executes a batch insert and maps the results (e.g. generated keys).
   * Used by SomeEntityDao.saveAll.
   */
  public <T, R> Flux<R> batch(Function<Connection, Statement> statementFactory,
                              Iterable<T> items,
                              BiConsumer<Statement, T> binder,
                              BiFunction<Row, RowMetadata, R> mapper) {
    return withConnection(conn ->
        Flux.from(prepareBatch(conn, statementFactory, items, binder).execute())
            .concatMap(result -> result.map(mapper))
    );
  }

  // -------------------------------------------------------------------------
  // Internal Helpers
  // -------------------------------------------------------------------------

  private <T> Statement prepareBatch(Connection conn,
                                     Function<Connection, Statement> factory,
                                     Iterable<T> items,
                                     BiConsumer<Statement, T> binder) {
    Statement statement = factory.apply(conn);
    Iterator<T> iterator = items.iterator();

    // Loop logic: Only call add() if we are moving to a NEW item after the first
    boolean first = true;
    while (iterator.hasNext()) {
      T item = iterator.next();
      if (!first) {
        statement.add();
      }
      binder.accept(statement, item);
      first = false;
    }
    return statement;
  }

  private void bindParameters(Statement statement, @Nullable Object[] parameters) {
    if (parameters == null) return;
    for (int i = 0; i < parameters.length; i++) {
      Object value = parameters[i];
      if (value != null) {
        statement.bind(i, value);
      } else {
        statement.bindNull(i, String.class);
      }
    }
  }
}