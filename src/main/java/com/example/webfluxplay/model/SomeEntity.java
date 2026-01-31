package com.example.webfluxplay.model;

import jakarta.validation.constraints.NotNull;

public record SomeEntity(Long id, @NotNull String svalue) {

  /**
   * Merges this entity with an existing one.
   * Since records are immutable, this returns a new instance.
   */
  public SomeEntity merge(SomeEntity existingEntity) {
    // Use local svalue if present, otherwise fall back to existingEntity's value
    String mergedSvalue = (this.svalue != null) ? this.svalue : existingEntity.svalue();

    return new SomeEntity(this.id, mergedSvalue);
  }
}