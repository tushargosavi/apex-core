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

import java.util.Collection;
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
  /* The original snapshot of logical plan */
  private LogicalPlan parent;

  List<DAG.OperatorMeta> removedOperators = Lists.newArrayList();
  List<DAG.StreamMeta> removedStreams = Lists.newArrayList();
  Map<String, LogicalPlan.OperatorMeta> newOperators = Maps.newHashMap();
  Map<String, TransactionStreamMeta> newStreams = Maps.newHashMap();
  Map<String, ExtendableStreamMeta> changedStreams = Maps.newHashMap();

  public DAGChangeTransactionImpl(LogicalPlan plan, int version)
  {
    parent = plan;
    this.transactionId = version;
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return new ReadOnlyAttributeMap(parent.getAttributes());
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return parent.getValue(key);
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
  public <T extends Operator> T addOperator(String name, Class<T> clazz)
  {
    T instance;
    try {
      instance = clazz.newInstance();
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
    addOperator(name, instance);
    return instance;
  }

  @Override
  public <T extends Operator> T addOperator(String name, T operator)
  {
    assertExistingOpeartor(name);
    return super.addOperator(name, operator);
  }

  private void assertExistingOpeartor(String name)
  {
    if (parent.getOperatorMeta(name) != null || super.operators.containsKey(name)) {
      LogicalPlan.OperatorMeta ometa  = parent.getOperatorMeta(name) != null ?
          parent.getOperatorMeta(name) : operators.get(name);
      throw new IllegalArgumentException("duplicate operator id: " + ometa.getName());
    }
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
  public DAG.StreamMeta addStream(String id)
  {
    assertExistingStream(id);
    TransactionStreamMeta meta = new TransactionStreamMeta(id, this);
    newStreams.put(id, meta);
    return meta;
  }

  private void assertExistingStream(Operator.OutputPort<?> port)
  {
    LogicalPlan.StreamMeta smeta = parent.getStreamBySource(port);
    if (smeta != null) {
      throw new IllegalArgumentException("Stream name already exists ");
    }
  }

  private void assertExistingStream(String id)
  {
    if (parent.getStream(id) != null) {
      throw new IllegalArgumentException("Stream already exists ");
    }
  }

  @Override
  public <T> DAG.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>[]
      sinks)
  {
    assertExistingStream(source);
    assertExistingStream(id);
    return super.addStream(id, source, sinks);
  }

  @Override
  public <T> DAG.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    assertExistingStream(id);
    assertExistingPort(source);
    return super.addStream(id, source, sink1);
  }

  @Override
  public <T> DAG.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>
      sink1, Operator.InputPort<? super T> sink2)
  {
    assertExistingStream(id);
    assertExistingStream(source);
    assertExistingPort(sink1);
    assertExistingPort(sink2);
    return super.addStream(id, source, sink1, sink2);
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    // can't change existing attributes
    throw new IllegalArgumentException("SetAttribute not allowed while in transaction ");
  }

  @Override
  public <T> void setAttribute(Operator operator, Attribute<T> key, T value)
  {
    assertExistingOpeator(operator);
    super.setAttribute(operator, key, value);
  }

  @Override
  public <T> void setOperatorAttribute(Operator operator, Attribute<T> key, T value)
  {
    assertExistingOpeator(operator);
    super.setAttribute(operator, key, value);
  }

  private void assertExistingOpeator(Operator operator)
  {
    if (operators.containsValue(operator)) {
      throw new IllegalArgumentException("Can not set attribute of existing opeator " + operator);
    }
  }

  @Override
  public <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertExistingPort(port);
    super.setOutputPortAttribute(port, key, value);
  }

  /**
   * check if port already exists in parent DAG and is connected to any stream.
   * @param outputPort
   */
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
    for (LogicalPlan.OperatorMeta o : getAllOperators()) {
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
    LogicalPlan.OutputPortMeta meta = parent.getPortMeta(port);
    if (meta == null) {
      meta = getLocalPortMeta(port);
    }
    return meta;
  }

  LogicalPlan.InputPortMeta getLocalPortMeta(Operator.InputPort<?> port)
  {
    for (LogicalPlan.OperatorMeta o : getAllOperators()) {
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
    LogicalPlan.InputPortMeta meta = parent.getPortMeta(port);
    if (meta == null) {
      meta = getLocalPortMeta(port);
    }
    return meta;
  }

  @Override
  public <T> void setUnifierAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    assertExistingPort(port);
    super.setUnifierAttribute(port, key, value);
  }

  @Override
  public <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value)
  {
    assertExistingPort(port);
    super.setInputPortAttribute(port, key, value);
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
    /**
     * Add newly added operators.
     */
    for (LogicalPlan.OperatorMeta ometa : getAllOperators()) {
      plan.addOperator(ometa);
    }

    /**
     * Add new streams.
     */
    LOG.info("Number of new streams {}", getAllStreams().size());
    for (DAG.StreamMeta psmeta : newStreams.values()) {
      plan.addStream(psmeta);
    }

    /**
     * Add newly connected port to existing streams.
     */
    for (ExtendableStreamMeta psmeta : changedStreams.values()) {
      DAG.StreamMeta smeta = plan.getStream(psmeta.getName());
      for (Operator.InputPort ip : psmeta.getNewSinks()) {
        smeta.addSink(ip);
      }
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

  public void validate() throws ConstraintViolationException
  {
    try {
      // copy plan for validation.
      LogicalPlan plan = parent.copy();
      merge(plan);
      plan.validate();
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }

    super.validate();
  }

  @Override
  public int getTransactionId()
  {
    return transactionId;
  }
}
