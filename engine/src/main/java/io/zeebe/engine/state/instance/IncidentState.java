/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbLong;
import io.zeebe.engine.metrics.IncidentMetrics;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import java.util.function.ObjLongConsumer;

public final class IncidentState {
  public static final int MISSING_INCIDENT = -1;

  /** incident key -> incident record */
  private final DbLong incidentKey;

  // we need two separate wrapper to not interfere with get and put
  // see https://github.com/zeebe-io/zeebe/issues/1916
  private final UnpackedObjectValue incidentRecordToRead;
  private final UnpackedObjectValue incidentRecordToWrite;
  private final ColumnFamily<DbLong, UnpackedObjectValue> incidentColumnFamily;

  /** element instance key -> incident key */
  private final DbLong elementInstanceKey;

  private final ColumnFamily<DbLong, Incident> workflowInstanceIncidentColumnFamily;
  private final Incident incident;

  /** job key -> incident key */
  private final DbLong jobKey;

  private final ColumnFamily<DbLong, Incident> jobIncidentColumnFamily;

  private final IncidentMetrics metrics;

  public IncidentState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final DbContext dbContext, final int partitionId) {
    incidentKey = new DbLong();
    incident = new Incident();

    incidentRecordToRead = new UnpackedObjectValue();
    incidentRecordToRead.wrapObject(new IncidentRecord());
    incidentRecordToWrite = new UnpackedObjectValue();
    incidentColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.INCIDENTS, dbContext, incidentKey, incidentRecordToRead);

    elementInstanceKey = new DbLong();
    workflowInstanceIncidentColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.INCIDENT_WORKFLOW_INSTANCES, dbContext, elementInstanceKey, incident);

    jobKey = new DbLong();
    jobIncidentColumnFamily =
        zeebeDb.createColumnFamily(ZbColumnFamilies.INCIDENT_JOBS, dbContext, jobKey, incident);

    metrics = new IncidentMetrics(partitionId);
  }

  public void createIncident(final long incidentKey, final IncidentRecord incident) {
    this.incidentKey.wrapLong(incidentKey);
    this.incidentRecordToWrite.wrapObject(incident);
    incidentColumnFamily.put(this.incidentKey, this.incidentRecordToWrite);

    this.incident.setKey(incidentKey);
    if (isJobIncident(incident)) {
      jobKey.wrapLong(incident.getJobKey());
      jobIncidentColumnFamily.put(jobKey, this.incident);
    } else {
      elementInstanceKey.wrapLong(incident.getElementInstanceKey());
      workflowInstanceIncidentColumnFamily.put(elementInstanceKey, this.incident);
    }

    metrics.incidentCreated();
  }

  public IncidentRecord getIncidentRecord(final long incidentKey) {
    this.incidentKey.wrapLong(incidentKey);

    final UnpackedObjectValue unpackedObjectValue = incidentColumnFamily.get(this.incidentKey);
    if (unpackedObjectValue != null) {
      return (IncidentRecord) unpackedObjectValue.getObject();
    }
    return null;
  }

  public void deleteIncident(final long key) {
    final IncidentRecord incidentRecord = getIncidentRecord(key);

    if (incidentRecord != null) {
      incidentColumnFamily.delete(incidentKey);

      if (isJobIncident(incidentRecord)) {
        jobKey.wrapLong(incidentRecord.getJobKey());
        jobIncidentColumnFamily.delete(jobKey);
      } else {
        elementInstanceKey.wrapLong(incidentRecord.getElementInstanceKey());
        workflowInstanceIncidentColumnFamily.delete(elementInstanceKey);
      }

      metrics.incidentResolved();
    }
  }

  public long getWorkflowInstanceIncidentKey(final long workflowInstanceKey) {
    elementInstanceKey.wrapLong(workflowInstanceKey);

    final Incident incident = workflowInstanceIncidentColumnFamily.get(elementInstanceKey);

    if (incident != null) {
      return incident.getKey();
    }

    return MISSING_INCIDENT;
  }

  public long getJobIncidentKey(final long jobKey) {
    this.jobKey.wrapLong(jobKey);
    final Incident incident = jobIncidentColumnFamily.get(this.jobKey);

    if (incident != null) {
      return incident.getKey();
    }
    return MISSING_INCIDENT;
  }

  public boolean isJobIncident(final IncidentRecord record) {
    return record.getJobKey() > 0;
  }

  public void forExistingWorkflowIncident(
      final long elementInstanceKey, final ObjLongConsumer<IncidentRecord> resolver) {
    final long workflowIncidentKey = getWorkflowInstanceIncidentKey(elementInstanceKey);

    final boolean hasIncident = workflowIncidentKey != IncidentState.MISSING_INCIDENT;
    if (hasIncident) {
      final IncidentRecord incidentRecord = getIncidentRecord(workflowIncidentKey);
      resolver.accept(incidentRecord, workflowIncidentKey);
    }
  }
}
