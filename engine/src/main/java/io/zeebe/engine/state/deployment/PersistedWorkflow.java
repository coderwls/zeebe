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
import io.zeebe.msgpack.property.BinaryProperty;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentResource;
import io.zeebe.protocol.impl.record.value.deployment.Workflow;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class PersistedWorkflow extends UnpackedObject implements DbValue {

  private final StringProperty processIdProp = new StringProperty("processId");
  private final StringProperty resourceNameProp = new StringProperty("resourceName");
  private final LongProperty keyProp = new LongProperty("key");
  private final IntegerProperty versionProp = new IntegerProperty("version");
  private final BinaryProperty resourceProp = new BinaryProperty("resource", new UnsafeBuffer());

  public PersistedWorkflow() {
    declareProperty(processIdProp)
        .declareProperty(resourceNameProp)
        .declareProperty(keyProp)
        .declareProperty(versionProp)
        .declareProperty(resourceProp);
  }

  public void wrap(
      final DeploymentResource resource, final Workflow workflow, final long workflowKey) {

    this.resourceProp.setValue(resource.getResourceBuffer());
    this.resourceNameProp.setValue(resource.getResourceNameBuffer());
    this.processIdProp.setValue(workflow.getBpmnProcessIdBuffer());

    this.versionProp.setValue(workflow.getVersion());
    this.keyProp.setValue(workflowKey);
  }

  @Override
  public void wrap(DirectBuffer buff) {
    wrap(buff, 0, buff.capacity());
  }

  @Override
  public void wrap(DirectBuffer buff, int offset, int length) {
    final byte[] bytes = new byte[length];
    final UnsafeBuffer mutableBuffer = new UnsafeBuffer(bytes);
    buff.getBytes(offset, bytes, 0, length);
    super.wrap(mutableBuffer, 0, length);
  }

  public int getVersion() {
    return versionProp.getValue();
  }

  public long getKey() {
    return keyProp.getValue();
  }

  public DirectBuffer getBpmnProcessId() {
    return processIdProp.getValue();
  }

  public DirectBuffer getResourceName() {
    return resourceNameProp.getValue();
  }

  public DirectBuffer getResource() {
    return resourceProp.getValue();
  }
}
