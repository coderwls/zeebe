/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.logstreams;

import io.zeebe.logstreams.state.Snapshot;
import io.zeebe.logstreams.state.SnapshotDeletionListener;
import io.zeebe.logstreams.state.SnapshotStorage;
import io.zeebe.util.sched.Actor;

public final class LogDeletionService extends Actor implements SnapshotDeletionListener {
  private final LogCompactor logCompactor;
  private final SnapshotStorage snapshotStorage;
  private final String actorName;

  public LogDeletionService(
      final int nodeId,
      final int partitionId,
      final LogCompactor logCompactor,
      final SnapshotStorage snapshotStorage) {
    this.snapshotStorage = snapshotStorage;
    this.logCompactor = logCompactor;
    actorName = buildActorName(nodeId, "DeletionService-" + partitionId);
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  protected void onActorStarting() {
    snapshotStorage.addDeletionListener(this);
  }

  @Override
  protected void onActorClosing() {
    if (snapshotStorage != null) {
      snapshotStorage.removeDeletionListener(this);
    }
  }

  @Override
  public void onSnapshotsDeleted(final Snapshot oldestRemainingSnapshot) {
    actor.run(() -> delegateDeletion(oldestRemainingSnapshot));
  }

  private void delegateDeletion(final Snapshot snapshot) {
    logCompactor.compactLog(snapshot.getCompactionBound()).join();
  }
}
