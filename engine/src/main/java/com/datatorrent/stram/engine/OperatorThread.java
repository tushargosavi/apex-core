/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.stram.api.ContainerEvent;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.plan.logical.Operators;
import com.datatorrent.stram.plan.logical.Operators.PortContextPair;

public class OperatorThread implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(StreamingContainer.class);
  private final OperatorDeployInfo ndi;
  private final ContainerContext1 cContext;
  private final Map<Integer, OperatorDeployInfo> nodeMap;
  private Node<?> node;
  private final HashSet<Integer> setOperators = new HashSet<>();

  public OperatorThread(OperatorDeployInfo ndi,
      Node<?> node,
      ContainerContext1 cContext,
      final Map<Integer, OperatorDeployInfo> nodeMap)
  {
    this.ndi = ndi;
    this.node = node;
    this.cContext = cContext;
    this.nodeMap = nodeMap;
  }

  @Override
  public void run()
  {
    OperatorDeployInfo currentdi = ndi;
    try {
      /* primary operator initialization */
      setupNode(currentdi);
      setOperators.add(currentdi.id);

      /* lets go for OiO operator initialization */
      List<Integer> oioNodeIdList = cContext.getOiOGroup(ndi.id);
      if (oioNodeIdList != null) {
        for (Integer oioNodeId : oioNodeIdList) {
          currentdi = nodeMap.get(oioNodeId);
          setupNode(currentdi);
          setOperators.add(currentdi.id);
        }
      }

      currentdi = null;

      node.run(); /* this is a blocking call */
    } catch (Error error) {
      handleError(error, currentdi);
    } catch (Exception ex) {
      handleException(ex, currentdi);
    } finally {
      tearDownNodes();
    }
  }

  private void handleException(Exception ex, OperatorDeployInfo currentdi)
  {
    if (currentdi == null) {
      cContext.setFailedNode(ndi.id);
      logger.error("Operator set {} stopped running due to an exception.", setOperators, ex);
      int[] operators = new int[]{ndi.id};
      try {
        cContext.reportError(operators, "Stopped running due to an exception. " + ExceptionUtils.getStackTrace(ex));
      } catch (Exception e) {
        logger.debug("Fail to log", e);
      }
    } else {
      cContext.setFailedNode(currentdi.id);
      logger.error("Abandoning deployment of operator {} due to setup failure.", currentdi, ex);
      int[] operators = new int[]{currentdi.id};
      try {
        cContext.reportError(operators, "Abandoning deployment due to setup failure. " + ExceptionUtils.getStackTrace(ex));
      } catch (Exception e) {
        logger.debug("Fail to log", e);
      }
    }
  }

  private void handleError(Error error, OperatorDeployInfo currentdi)
  {
    int[] operators;
    if (currentdi == null) {
      logger.error("Voluntary container termination due to an error in operator set {}.", setOperators, error);
      operators = new int[setOperators.size()];
      int i = 0;
      for (Integer it : setOperators) {
        operators[i] = it;
      }
    } else {
      logger.error("Voluntary container termination due to an error in operator {}.", currentdi, error);
      operators = new int[]{currentdi.id};
    }

    try {
      cContext.reportError(operators, "Voluntary container termination due to an error. " + ExceptionUtils.getStackTrace(error));
    } catch (Exception e) {
      logger.debug("Fail to log", e);
    } finally {
      System.exit(1);
    }
  }

  private void setupNode(OperatorDeployInfo ndi)
  {
    //failedNodes.remove(ndi.id);
    //final Node<?> node = nodes.get(ndi.id);

    node.setup(node.context);

    /* setup context for all the input ports */
    LinkedHashMap<String, PortContextPair<InputPort<?>>> inputPorts = node.getPortMappingDescriptor().inputPorts;
    LinkedHashMap<String, Operators.PortContextPair<InputPort<?>>> newInputPorts = new LinkedHashMap<>(inputPorts.size());
    for (OperatorDeployInfo.InputDeployInfo idi : ndi.inputs) {
      InputPort<?> port = inputPorts.get(idi.portName).component;
      PortContext context = new PortContext(idi.contextAttributes, node.context);
      newInputPorts.put(idi.portName, new PortContextPair<>(port, context));
      port.setup(context);
    }
    inputPorts.putAll(newInputPorts);

    /* setup context for all the output ports */
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> outputPorts = node.getPortMappingDescriptor().outputPorts;
    LinkedHashMap<String, PortContextPair<OutputPort<?>>> newOutputPorts = new LinkedHashMap<>(outputPorts.size());
    for (OperatorDeployInfo.OutputDeployInfo odi : ndi.outputs) {
      OutputPort<?> port = outputPorts.get(odi.portName).component;
      PortContext context = new PortContext(odi.contextAttributes, node.context);
      newOutputPorts.put(odi.portName, new PortContextPair<>(port, context));
      port.setup(context);
    }
    outputPorts.putAll(newOutputPorts);

    /* This introduces need for synchronization on processNodeRequest which was solved by adding deleted field in StramToNodeRequest  */
    cContext.processNodeRequests(false);
    node.activate();
    cContext.publish(new ContainerEvent.NodeActivationEvent(node));
  }


  /**
   * Teardown operators managed by this thread.
   */
  private void tearDownNodes()
  {
    // tear down operators, which had completed setup.
    for (Integer id : setOperators) {
      try {
        final Node<?> node = cContext.getNode(id);
        if (node == null) {
          logger.warn("node {}/{} took longer to exit, resulting in unclean undeploy!", ndi.id, ndi.name);
        } else {
          cContext.publish(new ContainerEvent.NodeDeactivationEvent(node));
          node.deactivate();
          node.teardown();
          logger.debug("deactivated {}", node.getId());
        }
      } catch (Exception ex) {
        cContext.setFailedNode(id);
        logger.error("Shutdown of operator {} failed due to an exception.", id, ex);
      }
    }
  }
}
