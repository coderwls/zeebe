/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import io.atomix.protocols.raft.partition.RaftPartition;
import io.zeebe.util.health.FailureListener;
import io.zeebe.util.health.HealthMonitorable;
import io.zeebe.util.health.HealthStatus;

public class RaftPartitionHealth implements HealthMonitorable {

  private final RaftPartition atomixRaftPartition;

  public RaftPartitionHealth(final RaftPartition atomixRaftPartition) {
    this.atomixRaftPartition = atomixRaftPartition;
  }

  @Override
  public HealthStatus getHealthStatus() {
    final boolean isHealthy = atomixRaftPartition.getServer().isRunning();
    return isHealthy ? HealthStatus.HEALTHY : HealthStatus.UNHEALTHY;
  }

  @Override
  public void addFailureListener(final FailureListener failureListener) {
    atomixRaftPartition.addFailureListener(failureListener::onFailure);
  }
}
