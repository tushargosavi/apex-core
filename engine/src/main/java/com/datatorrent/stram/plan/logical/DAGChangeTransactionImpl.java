/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.plan.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;

public class DAGChangeTransactionImpl extends LogicalPlan implements DAG.DAGChangeTransaction
{
  private static final Logger LOG = LoggerFactory.getLogger(LogicalPlan.class);

  final int transactionId;

  /* The reference to original DAG */
  private final LogicalPlan parent;
  /* The clone of parent */
  private final LogicalPlan cloned;

  List<DAG.OperatorMeta> removedOperators = Lists.newArrayList();
  List<DAG.StreamMeta> removedStreams = Lists.newArrayList();

  public DAGChangeTransactionImpl(LogicalPlan plan, int version)
  {
    parent = plan;
    cloned = plan.copy();
    this.transactionId = version;
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return new ReadOnlyAttributeMap(cloned.getAttributes());
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return cloned.getValue(key);
  }

  @Override
  public void setCounters(Object counters)
  {
    // not implemented.
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    // not implemented.
  }

  @Override
  public <T extends Module> T addModule(String name, Class<T> moduleClass)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T extends Module> T addModule(String name, T module)
  {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    // can't change existing attributes
    throw new IllegalArgumentException("SetAttribute not allowed while in transaction ");
  }

  private void assertExistingPort(Operator.OutputPort<?> outputPort)
  {
    LogicalPlan.OutputPortMeta meta = parent.getPortMeta(outputPort);
    if (meta != null) {
      // check if port is already connected
      boolean alreadyConnected = meta.getOperatorMeta().getOutputStreams().containsKey(meta);
      if (alreadyConnected) {
        throw new IllegalArgumentException("Port already connected in parent dag ");
      }
    }
  }

  private void assertExistingPort(Operator.InputPort<?> inputPort)
  {
    LogicalPlan.InputPortMeta meta = parent.getPortMeta(inputPort);
    if (meta != null) {
      // check if port is already connected
      boolean alreadyConnected = meta.getOperatorMeta().getInputStreams().containsKey(meta);
      if (alreadyConnected) {
        throw new IllegalArgumentException("Port already connected in parent dag ");
      }
    }
  }

  LogicalPlan.OutputPortMeta getLocalPortMeta(Operator.OutputPort<?> port)
  {
    for (LogicalPlan.OperatorMeta o : operators.values()) {
      LogicalPlan.OutputPortMeta opm = o.getPortMapping().getOutPortMap().get(port);
      if (opm != null) {
        return opm;
      }
    }
    return null;
  }

  @Override
  LogicalPlan.OutputPortMeta getPortMeta(Operator.OutputPort<?> port)
  {
    LogicalPlan.OutputPortMeta meta = cloned.getPortMeta(port);
    if (meta == null) {
      meta = getLocalPortMeta(port);
    }
    return meta;
  }

  LogicalPlan.InputPortMeta getLocalPortMeta(Operator.InputPort<?> port)
  {
    for (LogicalPlan.OperatorMeta o : operators.values()) {
      LogicalPlan.InputPortMeta opm = o.getPortMapping().getInPortMap().get(port);
      if (opm != null) {
        return opm;
      }
    }
    return null;
  }

  @Override
  public LogicalPlan.InputPortMeta getPortMeta(Operator.InputPort<?> port)
  {
    LogicalPlan.InputPortMeta meta = cloned.getPortMeta(port);
    if (meta == null) {
      meta = getLocalPortMeta(port);
    }
    return meta;
  }

  public void commit()
  {
    parent.commit(this);
  }

  /**
   * Merge the delta into given plan.
   * @param plan
   */
  void merge(LogicalPlan plan)
  {

//    /**
//     * process removed operator first
//     * TODO: remove of operators needs to be from output to input.
//     */
//    for (DAG.OperatorMeta ometa : removedOperators) {
//      plan.removeOperator(ometa.getOperator());
//    }

    /**
     * Add newly added operators.
     */
    for (LogicalPlan.OperatorMeta ometa : operators.values()) {
      plan.addOperator(ometa);
    }

    /**
     * Add stream, existing streams will be modified too.
     */
    for (DAG.StreamMeta psmeta : streams.values()) {
      plan.addStream(psmeta);
    }
  }

  @Override
  public void removeOperator(String name)
  {
    removedOperators.add(getOperatorMeta(name));
  }

  @Override
  public <T extends DAG.OperatorMeta> void removeOperator(T meta)
  {
    removedOperators.add(meta);
  }

  @Override
  public void removeStream(String name)
  {
    removedStreams.add(getStream(name));
  }

  @Override
  public <T extends DAG.StreamMeta> void removeStream(T streamMeta)
  {
    removedStreams.add(streamMeta);
  }

  @Override
  public DAG.StreamMeta getStream(String id)
  {
    DAG.StreamMeta smeta = streams.get(id);
    if (smeta != null) {
      return smeta;
    }
    return cloned.getStream(id);
  }

  @Override
  public List<LogicalPlan.OperatorMeta> getRootOperators()
  {
    List<LogicalPlan.OperatorMeta> thisrootOperators = super.getRootOperators();
    List<LogicalPlan.OperatorMeta> rootOperators = new ArrayList<>();
    rootOperators.addAll(thisrootOperators);
    rootOperators.addAll(cloned.getRootOperators());
    return Collections.unmodifiableList(rootOperators);
  }

  @Override
  public List<LogicalPlan.OperatorMeta> getRootOperatorsMeta()
  {
    List<LogicalPlan.OperatorMeta> rootOperators = super.getRootOperatorsMeta();
    rootOperators.addAll(cloned.getRootOperatorsMeta());
    return rootOperators;
  }

  @Override
  public List<LogicalPlan.OperatorMeta> getLeafOperators()
  {
    List<LogicalPlan.OperatorMeta> leafOperators = super.getLeafOperators();
    leafOperators.addAll(cloned.getLeafOperators());
    return leafOperators;
  }

  @Override
  public Collection<LogicalPlan.OperatorMeta> getAllOperators()
  {
    Collection<LogicalPlan.OperatorMeta> allOperators = super.getAllOperators();
    allOperators.addAll(cloned.getAllOperators());
    return allOperators;
  }

  @Override
  public Collection<LogicalPlan.OperatorMeta> getAllOperatorsMeta()
  {
    return super.getAllOperatorsMeta();
  }

  @Override
  public Collection<LogicalPlan.ModuleMeta> getAllModules()
  {
    return super.getAllModules();
  }

  @Override
  public Collection<LogicalPlan.StreamMeta> getAllStreams()
  {
    return super.getAllStreams();
  }

  @Override
  public Collection<LogicalPlan.StreamMeta> getAllStreamsMeta()
  {
    return super.getAllStreamsMeta();
  }

  @Override
  public LogicalPlan.OperatorMeta getOperatorMeta(String operatorName)
  {
    LogicalPlan.OperatorMeta ometa = super.getOperatorMeta(operatorName);
    if (ometa != null) {
      return ometa;
    }
    return cloned.getOperatorMeta(operatorName);
  }

  @Override
  public LogicalPlan.OperatorMeta getMeta(Operator operator)
  {
    LogicalPlan.OperatorMeta ometa = super.getMeta(operator);
    if (ometa != null) {
      return ometa;
    }
    return cloned.getMeta(operator);
  }

  public void validate() throws ConstraintViolationException
  {
    LogicalPlan plan = parent.copy();
    merge(plan);
    plan.validate();
  }

  @Override
  public int getTransactionId()
  {
    return transactionId;
  }
}
