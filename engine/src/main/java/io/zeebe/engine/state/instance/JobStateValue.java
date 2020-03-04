/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.EnumProperty;

public class JobStateValue extends UnpackedObject implements DbValue {

  private final EnumProperty<JobState.State> jobStateProp =
      new EnumProperty<>("jobState", JobState.State.class);

  public JobStateValue() {
    declareProperty(jobStateProp);
  }

  public JobState.State get() {
    return jobStateProp.getValue();
  }

  public void set(JobState.State state) {
    jobStateProp.setValue(state);
  }
}
