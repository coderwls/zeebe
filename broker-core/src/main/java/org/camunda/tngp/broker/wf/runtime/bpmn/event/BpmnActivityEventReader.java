package org.camunda.tngp.broker.wf.runtime.bpmn.event;

import org.camunda.tngp.graph.bpmn.ExecutionEventType;
import org.camunda.tngp.taskqueue.data.BpmnActivityEventDecoder;
import org.camunda.tngp.taskqueue.data.MessageHeaderDecoder;
import org.camunda.tngp.util.buffer.BufferReader;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class BpmnActivityEventReader implements BufferReader
{

    protected final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    protected final BpmnActivityEventDecoder bodyDecoder = new BpmnActivityEventDecoder();
    protected final UnsafeBuffer taskTypeBuffer = new UnsafeBuffer(0, 0);

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length)
    {
        headerDecoder.wrap(buffer, offset);

        offset += headerDecoder.encodedLength();

        bodyDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        offset += bodyDecoder.encodedLength();
        offset +=  BpmnActivityEventDecoder.taskTypeHeaderLength();
        final int taskTypeLength = bodyDecoder.taskTypeLength();

        taskTypeBuffer.wrap(buffer, offset, taskTypeLength);
    }

    public long key()
    {
        return bodyDecoder.key();
    }

    public long wfDefinitionId()
    {
        return bodyDecoder.wfDefinitionId();
    }

    public ExecutionEventType event()
    {
        return ExecutionEventType.get(bodyDecoder.event());
    }

    public int flowElementId()
    {
        return bodyDecoder.flowElementId();
    }

    public long wfInstanceId()
    {
        return bodyDecoder.wfInstanceId();
    }

    public int taskQueueId()
    {
        return bodyDecoder.taskQueueId();
    }

    public int resourceId()
    {
        return headerDecoder.resourceId();
    }

    public DirectBuffer getTaskType()
    {
        return taskTypeBuffer;
    }
}
