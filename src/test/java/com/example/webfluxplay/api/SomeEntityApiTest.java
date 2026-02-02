package com.example.webfluxplay.api;

import com.example.webfluxplay.dao.SomeEntityDao;
import com.example.webfluxplay.model.SomeEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class SomeEntityApiTest {

  @Autowired
  private WebTestClient webTestClient;

  @Autowired
  private SomeEntityDao dao;

  @BeforeEach
  void setUp() {
    // Clean DB before each API test
    dao.createTable().blockLast();
    dao.findAll()
        .flatMap(e -> dao.deleteById(e.getId()))
        .blockLast();
  }

  @Test
  void getAll_shouldReturnEmpty_initially() {
    webTestClient.get().uri("/api/someentity")
        .exchange()
        .expectStatus().isOk()
        .expectBodyList(SomeEntity.class).hasSize(0);
  }

  @Test
  void create_shouldReturnCreated_andResource() {
    SomeEntity payload = new SomeEntity();
    payload.setSvalue("api-test");

    webTestClient.post().uri("/api/someentity")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Mono.just(payload), SomeEntity.class)
        .exchange()
        .expectStatus().isCreated()
        .expectHeader().valueMatches("Location", ".*/api/someentity/\\d+")
        .expectBody()
        .jsonPath("$.id").isNotEmpty()
        .jsonPath("$.svalue").isEqualTo("api-test");
  }

  @Test
  void create_shouldReturnBadRequest_whenValidationFails() {
    SomeEntity payload = new SomeEntity();
    payload.setSvalue(null); // @NotNull violation

    webTestClient.post().uri("/api/someentity")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Mono.just(payload), SomeEntity.class)
        .exchange()
        .expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.message").exists(); // Assuming MessageErrorAttributes logic
  }

  @Test
  void getById_shouldReturnOne() {
    // Seed
    SomeEntity seed = new SomeEntity();
    seed.setSvalue("seed");
    SomeEntity saved = dao.save(seed).block();

    webTestClient.get().uri("/api/someentity/" + saved.getId())
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.id").isEqualTo(saved.getId())
        .jsonPath("$.svalue").isEqualTo("seed");
  }

  @Test
  void patch_shouldUpdateEntity() {
    // Seed
    SomeEntity seed = new SomeEntity();
    seed.setSvalue("old");
    SomeEntity saved = dao.save(seed).block();

    // Update
    SomeEntity patch = new SomeEntity();
    patch.setId(saved.getId());
    patch.setSvalue("new");

    webTestClient.patch().uri("/api/someentity")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Mono.just(patch), SomeEntity.class)
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.svalue").isEqualTo("new");
  }

  @Test
  void createBatch_shouldHandleList() {
    List<SomeEntity> batch = IntStream.range(0, 10)
        .mapToObj(i -> {
          SomeEntity e = new SomeEntity();
          e.setSvalue("batch-" + i);
          return e;
        })
        .collect(Collectors.toList());

    webTestClient.post().uri("/api/someentity/all")
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(batch)
        .exchange()
        .expectStatus().isOk()
        .expectBodyList(SomeEntity.class).hasSize(10);
  }
}