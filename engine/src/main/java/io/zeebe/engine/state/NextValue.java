/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;

public class NextValue extends UnpackedObject implements DbValue {

  private final LongProperty valueProp = new LongProperty("nextValue");

  public NextValue() {
    declareProperty(valueProp);
  }

  public void set(final long value) {
    valueProp.setValue(value);
  }

  public long get() {
    return valueProp.getValue();
  }
}
