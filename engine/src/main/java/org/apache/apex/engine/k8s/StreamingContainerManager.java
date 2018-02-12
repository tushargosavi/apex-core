/**
 * Copyright (c) 2012-2018 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.k8s;

import java.lang.management.ManagementFactory;

import org.apache.apex.engine.api.Clock;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.ContainerInfo;

/**
 * Created by sergey on 2/2/18.
 */
public class StreamingContainerManager extends com.datatorrent.stram.StreamingContainerManager
{
  public StreamingContainerManager(LogicalPlan dag, Clock clock)
  {
    super(dag, clock);
  }

  public StreamingContainerManager(LogicalPlan dag)
  {
    super(dag);
  }

  public StreamingContainerManager(LogicalPlan dag, boolean enableEventRecording, Clock clock)
  {
    super(dag, enableEventRecording, clock);
  }

  public StreamingContainerManager(CheckpointState checkpointedState, boolean enableEventRecording)
  {
    super(checkpointedState, enableEventRecording);
  }

  @Override
  public ContainerInfo getAppMasterContainerInfo()
  {
    ContainerInfo ci = new ContainerInfo();
    ci.id = "id_0001";
    ci.state = "ACTIVE";
    ci.jvmName = ManagementFactory.getRuntimeMXBean().getName();
    ci.numOperators = 0;
    ci.memoryMBAllocated = allocatedMemoryMB;
    ci.memoryMBFree = ((int)(Runtime.getRuntime().freeMemory() / (1024 * 1024)));
    ci.lastHeartbeat = -1;
    ci.startedTime = startTime;
    ci.finishedTime = -1;
    return ci;
  }
}
