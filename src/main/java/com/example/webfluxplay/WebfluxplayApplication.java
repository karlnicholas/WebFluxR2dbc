package com.example.webfluxplay;

import com.example.webfluxplay.dao.SomeEntityDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, R2dbcAutoConfiguration.class})
@Slf4j
public class WebfluxplayApplication {

  private final SomeEntityDao dao;

  public WebfluxplayApplication(SomeEntityDao dao) {
    this.dao = dao;
  }

  public static void main(String[] args) {
    SpringApplication.run(WebfluxplayApplication.class, args);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void doSomethingAfterStartup() {
    log.info("App started. Initializing database..."); // 1. Verify the listener runs

    dao.createTable()
        .doOnNext(i -> log.info("Rows updated: " + i))
        .doOnComplete(() -> log.info("Table creation completed successfully.")) // 2. Handle empty success
        .doOnError(err -> log.error("Failed to create table", err))
        .blockLast(); // 3. Block to keep the main thread alive for this task
  }
}