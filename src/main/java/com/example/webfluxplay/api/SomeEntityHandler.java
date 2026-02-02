package com.example.webfluxplay.api;

import com.example.webfluxplay.dao.SomeEntityDao;
import com.example.webfluxplay.dao.SomeEntityMsDao;
import com.example.webfluxplay.model.SomeEntity;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
public class SomeEntityHandler {

  private final Validator validator;
  private final SomeEntityMsDao dao;

  public SomeEntityHandler(Validator validator, SomeEntityMsDao dao) {
    this.validator = validator;
    this.dao = dao;
  }

  // -----------------------------------------------------------------------
  // READ
  // -----------------------------------------------------------------------

  public Mono<ServerResponse> listSomeEntities(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(dao.findAll(), SomeEntity.class);
  }

  public Mono<ServerResponse> getSomeEntity(ServerRequest request) {
    Long id = Long.valueOf(request.pathVariable("id"));

    return dao.findById(id)
        .flatMap(entity -> ServerResponse.ok()
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(entity))
        .switchIfEmpty(ServerResponse.notFound().build());
  }

  // -----------------------------------------------------------------------
  // WRITE
  // -----------------------------------------------------------------------

  public Mono<ServerResponse> createSomeEntity(ServerRequest request) {
    return request.bodyToMono(SomeEntity.class)
        .doOnNext(this::validate)
        .flatMap(dao::save)
        .flatMap(saved -> ServerResponse.created(request.uriBuilder()
                .path("/{id}").build(saved.getId())) // Record accessor .id()
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(saved));
  }

  public Mono<ServerResponse> createSomeEntities(ServerRequest request) {
    return ServerResponse.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(request.bodyToFlux(SomeEntity.class)
            .buffer(50) // Process in batches of 50
            .flatMap(batch -> {
              validateAll(batch);
              return dao.saveAll(batch);
            }), SomeEntity.class);
  }

  public Mono<ServerResponse> updateSomeEntity(ServerRequest request) {
    return request.bodyToMono(SomeEntity.class)
        .doOnNext(this::validate)
        // Delegate to the atomic DAO method
        .flatMap(dao::update)
        .flatMap(saved -> ServerResponse.ok()
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(saved))
        .onErrorResume(IllegalArgumentException.class, e -> ServerResponse.notFound().build());
  }

  public Mono<ServerResponse> deleteSomeEntity(ServerRequest request) {
    Long id = Long.valueOf(request.pathVariable("id"));
    return dao.deleteById(id)
        .then(ServerResponse.noContent().build());
  }

  // -----------------------------------------------------------------------
  // VALIDATION
  // -----------------------------------------------------------------------

  private void validate(SomeEntity someEntity) {
    Errors errors = new BeanPropertyBindingResult(someEntity, "SomeEntity");
    validator.validate(someEntity, errors);
    if (errors.hasErrors()) {
      throw new ServerWebInputException(errors.toString());
    }
  }

  private void validateAll(List<SomeEntity> someEntities) {
    // For lists, we just iterate and validate each
    // Alternatively, you could use a custom List wrapper validator
    Errors errors = new BeanPropertyBindingResult(someEntities, "SomeEntityList");
    for (int i = 0; i < someEntities.size(); i++) {
      validator.validate(someEntities.get(i), errors);
    }
    if (errors.hasErrors()) {
      throw new ServerWebInputException(errors.toString());
    }
  }
}