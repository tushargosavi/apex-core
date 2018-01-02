/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.local;

import java.lang.management.ManagementFactory;

import org.apache.apex.engine.api.Clock;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.webapp.ContainerInfo;

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
    ci.id = "local_0001";
    // TODO: Commenting these out for now, will replace as needed
    /*
    ci.id = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString());
    String nmHost = System.getenv(ApplicationConstants.Environment.NM_HOST.toString());
    String nmPort = System.getenv(ApplicationConstants.Environment.NM_PORT.toString());
    String nmHttpPort = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString());
    */
    ci.state = "ACTIVE";
    ci.jvmName = ManagementFactory.getRuntimeMXBean().getName();
    ci.numOperators = 0;
    /*
    Configuration conf = ClusterProviderFactory.getProvider().getConfiguration();
    if (nmHost != null) {
      if (nmPort != null) {
        ci.host = nmHost + ":" + nmPort;
      }
      if (nmHttpPort != null) {
        String nodeHttpAddress = nmHost + ":" + nmHttpPort;
        if (allocatedMemoryMB == 0) {
          String url = ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/ws/v1/node/containers/" + ci.id;
          try (YarnClient rmClient = YarnClient.createYarnClient()) {
            rmClient.init(conf);
            rmClient.start();
            ContainerReport content = rmClient.getContainerReport(ContainerId.fromString(ci.id));
            int totalMemoryNeededMB = content.getAllocatedResource().getMemory();
            LOG.debug("App Master allocated memory is {}", totalMemoryNeededMB);
            if (totalMemoryNeededMB > 0) {
              allocatedMemoryMB = totalMemoryNeededMB;
            } else {
              LOG.warn("Could not determine the memory allocated for the streaming application master.  Node manager is reporting {} MB from {}", totalMemoryNeededMB, url);
            }
          } catch (Exception ex) {
            LOG.warn("Could not determine the memory allocated for the streaming application master", ex);
          }
        }
        ci.containerLogsUrl = ConfigUtils.getSchemePrefix(conf) + nodeHttpAddress + "/node/containerlogs/" + ci.id + "/" + System.getenv(ApplicationConstants.Environment.USER.toString());
        ci.rawContainerLogsUrl = ConfigUtils.getRawContainerLogsUrl(conf, nodeHttpAddress, plan.getLogicalPlan().getAttributes().get(LogicalPlan.APPLICATION_ID), ci.id);
      }
    }
    */

    ci.memoryMBAllocated = allocatedMemoryMB;
    ci.memoryMBFree = ((int)(Runtime.getRuntime().freeMemory() / (1024 * 1024)));
    ci.lastHeartbeat = -1;
    ci.startedTime = startTime;
    ci.finishedTime = -1;
    return ci;
  }
}
