package com.example.webfluxplay.model;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SomeEntityTest {

  @Test
  void merge_shouldUseExistingValue_whenPayloadValueIsNull() {
    // Arrange
    SomeEntity existing = new SomeEntity();
    existing.setId(1L);
    existing.setSvalue("original");

    SomeEntity payload = new SomeEntity();
    payload.setId(1L);
    payload.setSvalue(null); // The update payload has no value

    // Act
    SomeEntity result = payload.merge(existing);

    // Assert
    assertThat(result.getSvalue()).isEqualTo("original");
  }

  @Test
  void merge_shouldUsePayloadValue_whenPayloadValueIsNotNull() {
    // Arrange
    SomeEntity existing = new SomeEntity();
    existing.setId(1L);
    existing.setSvalue("original");

    SomeEntity payload = new SomeEntity();
    payload.setId(1L);
    payload.setSvalue("updated");

    // Act
    SomeEntity result = payload.merge(existing);

    // Assert
    assertThat(result.getSvalue()).isEqualTo("updated");
  }
}