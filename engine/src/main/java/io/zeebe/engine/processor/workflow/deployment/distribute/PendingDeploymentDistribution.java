/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.distribute;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.DocumentProperty;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import org.agrona.DirectBuffer;

public class PendingDeploymentDistribution extends UnpackedObject implements DbValue {

  private final LongProperty sourcePositionProp = new LongProperty("sourcePosition");
  private final IntegerProperty distributionCountProp = new IntegerProperty("distributionCount");
  private final DocumentProperty deploymentProp = new DocumentProperty("deployment");

  public PendingDeploymentDistribution(
      final DirectBuffer deployment, final long sourcePosition, final int distributionCount) {
    declareProperty(sourcePositionProp)
        .declareProperty(distributionCountProp)
        .declareProperty(deploymentProp);

    sourcePositionProp.setValue(sourcePosition);
    distributionCountProp.setValue(distributionCount);
    deploymentProp.setValue(deployment);
  }

  public int decrementCount() {
    final int decremented = distributionCountProp.getValue() - 1;
    distributionCountProp.setValue(decremented);
    return decremented;
  }

  public DirectBuffer getDeployment() {
    return deploymentProp.getValue();
  }

  public long getSourcePosition() {
    return sourcePositionProp.getValue();
  }
}
