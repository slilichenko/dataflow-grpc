package com.google.solutions.dataflow.grpc.model;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PartialAddress implements Serializable {
  public abstract String getZip();
  public abstract String getState();
  public abstract String getCity();

  public static PartialAddress create(String newZip, String newState, String newCity) {
    return builder()
        .setZip(newZip)
        .setState(newState)
        .setCity(newCity)
        .build();
  }

  public static Builder builder() {
    return new AutoValue_PartialAddress.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setZip(String newZip);

    public abstract Builder setState(String newState);

    public abstract Builder setCity(String newCity);

    public abstract PartialAddress build();
  }
}
