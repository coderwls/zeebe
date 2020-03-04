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
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.ObjectProperty;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class IndexedRecord extends UnpackedObject implements DbValue {

  private final ObjectProperty<WorkflowInstanceRecord> recordProp =
      new ObjectProperty<>("record", new WorkflowInstanceRecord());
  private final LongProperty keyProp = new LongProperty("key", -1);
  private final EnumProperty<WorkflowInstanceIntent> stateProp =
      new EnumProperty<>("state", WorkflowInstanceIntent.class);

  IndexedRecord() {
    declareProperty(keyProp).declareProperty(stateProp).declareProperty(recordProp);
  }

  public IndexedRecord(
      final long key,
      final WorkflowInstanceIntent instanceState,
      final WorkflowInstanceRecord record) {
    this();

    keyProp.setValue(key);
    stateProp.setValue(instanceState);
    setValue(record);
  }

  public long getKey() {
    return keyProp.getValue();
  }

  public IndexedRecord setKey(final long key) {
    keyProp.setValue(key);
    return this;
  }

  public WorkflowInstanceIntent getState() {
    return stateProp.getValue();
  }

  public IndexedRecord setState(final WorkflowInstanceIntent state) {
    stateProp.setValue(state);
    return this;
  }

  public WorkflowInstanceRecord getValue() {
    return recordProp.getValue();
  }

  public IndexedRecord setValue(final WorkflowInstanceRecord value) {
    recordProp.getValue().wrap(value);
    return this;
  }

  public void wrap(final IndexedRecord indexedRecord) {
    setKey(indexedRecord.getKey());
    setState(indexedRecord.getState());
    setValue(indexedRecord.getValue());
  }

  // TODO: refactor Db wraps
  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    final byte[] bytes = new byte[length];
    final UnsafeBuffer mutableBuffer = new UnsafeBuffer(bytes);
    buffer.getBytes(offset, bytes, 0, length);
    super.wrap(mutableBuffer, 0, length);
  }

  @Override
  public String toString() {
    return "IndexedRecord{"
        + "key="
        + keyProp.getValue()
        + ", state="
        + stateProp.getValue()
        + ", value="
        + recordProp.getValue()
        + '}';
  }
}
