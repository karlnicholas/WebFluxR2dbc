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

  public WebfluxplayApplication(SomeEntityDao dao) {
    this.dao = dao;
  }

  public static void main(String[] args) {
    SpringApplication.run(WebfluxplayApplication.class, args);
  }


  private final SomeEntityDao dao;

  @EventListener(ApplicationReadyEvent.class)
  public void doSomethingAfterStartup() {
    dao.createTable().subscribe(i -> log.info("Table created: " + i));
  }

}
