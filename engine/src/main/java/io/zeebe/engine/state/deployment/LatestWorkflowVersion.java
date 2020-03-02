/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.deployment;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.LongProperty;

public class LatestWorkflowVersion extends UnpackedObject implements DbValue {
  private final LongProperty workflowVersionProp = new LongProperty("workflowVersion");

  public LatestWorkflowVersion() {
    declareProperty(workflowVersionProp);
  }

  public long getWorkflowVersion() {
    return workflowVersionProp.getValue();
  }

  public void setWorkflowVersion(final long version) {
    workflowVersionProp.setValue(version);
  }
}
