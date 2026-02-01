package com.example.webfluxplay.model;

import jakarta.validation.constraints.NotNull;

public class SomeEntity { // CLASS, not RECORD
  private Long id;

  @NotNull
  private String svalue;

  public Long getId() { return id; }
  public void setId(Long id) { this.id = id; }

  public String getSvalue() { return svalue; }
  public void setSvalue(String svalue) { this.svalue = svalue; }

  public SomeEntity merge(SomeEntity existingEntity) {
    if (svalue == null) {
      svalue = existingEntity.getSvalue();
    }
    return this;
  }
}