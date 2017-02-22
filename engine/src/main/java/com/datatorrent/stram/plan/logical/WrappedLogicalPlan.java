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
import java.util.List;
import java.util.Map;

import javax.validation.ConstraintViolationException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;

public class WrappedLogicalPlan extends LogicalPlan implements DAG
{
  final int parentVersion;
  /* The original snapshot of logical plan */
  private LogicalPlan parent;

  List<LogicalPlan.OperatorMeta> removedOperators = Lists.newArrayList();
  Map<String, LogicalPlan.OperatorMeta> newOperators = Maps.newHashMap();
  Map<String, LogicalPlan.StreamMeta> newStreams = Maps.newHashMap();
  Map<String, WrappedStreamMeta> changedStreams = Maps.newHashMap();

  public WrappedLogicalPlan(LogicalPlan plan, int version)
  {
    parent = plan;
    this.parentVersion = version;
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return parent.getAttributes();
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
  public LogicalPlan.StreamMeta addStream(String id)
  {
    assertExistingStream(id);
    return super.addStream(id);
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
  public <T> LogicalPlan.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>[]
      sinks)
  {
    assertExistingStream(source);
    assertExistingStream(id);
    return super.addStream(id, source, sinks);
  }

  @Override
  public <T> LogicalPlan.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    assertExistingStream(id);
    assertExistingPort(source);
    return super.addStream(id, source, sink1);
  }

  @Override
  public <T> LogicalPlan.StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>
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
  LogicalPlan.InputPortMeta getPortMeta(Operator.InputPort<?> port)
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


  void merge(LogicalPlan plan)
  {
    for (LogicalPlan.OperatorMeta ometa : getAllOperators()) {
      plan.addOperator(ometa);
    }

    for (LogicalPlan.StreamMeta psmeta : getAllStreams()) {
      plan.addStream(psmeta);
    }

  }

  /**
   * A wrapper class to track stream extension. A stream can be extended by adding more
   * sink to it.
   */
  private class WrappedStreamMeta implements DAG.StreamMeta
  {

    /* the original stream meta */
    final LogicalPlan.StreamMeta parent;
    private List<Operator.InputPort<?>> newSinks = new ArrayList<>();

    public WrappedStreamMeta(LogicalPlan.StreamMeta parent)
    {
      this.parent = parent;
    }

    @Override
    public String getName()
    {
      return parent.getName();
    }

    @Override
    public Locality getLocality()
    {
      return parent.getLocality();
    }

    @Override
    public DAG.StreamMeta setLocality(Locality locality)
    {
      throw new UnsupportedOperationException("Can not set locality of existing stream");
    }

    @Override
    public DAG.StreamMeta setSource(Operator.OutputPort<?> port)
    {
      throw new UnsupportedOperationException("Can not set source of existing stream");
    }

    @Override
    public DAG.StreamMeta addSink(Operator.InputPort<?> port)
    {
      newSinks.add(port);
      return this;
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public DAG.StreamMeta persistUsing(String name, Operator persistOperator, Operator.InputPort<?> persistOperatorInputPort, Operator.InputPort<?> sinkToPersist)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends DAG.OutputPortMeta> T getSource()
    {
      return (T)parent.getSource();
    }

    @Override
    public <T extends DAG.InputPortMeta> Collection<T> getSinks()
    {
      // return null;
      throw new RuntimeException("not implemented");
    }
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
}
