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

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Module;
import com.datatorrent.api.Operator;

/**
 * A read-only wrapper around LogicalPlan
 */
public class ReadOnlyLogicalPlan implements DAG
{
  private final LogicalPlan parent;
  private ReadOnlyAttributeMap readOnlyAttributes;

  public ReadOnlyLogicalPlan(LogicalPlan parent)
  {
    this.parent = parent;
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    if (readOnlyAttributes == null) {
      readOnlyAttributes = new ReadOnlyAttributeMap(parent.getAttributes());
    }
    return readOnlyAttributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return parent.getValue(key);
  }

  @Override
  public void setCounters(Object counters)
  {
    // No-op
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    // No-op
  }

  @Override
  public <T extends Operator> T addOperator(String name, Class<T> clazz)
  {
    throw new RuntimeException("addOperator not permitted on the read-only DAG");
  }

  @Override
  public <T extends Operator> T addOperator(String name, T operator)
  {
    throw new RuntimeException("addOperator not permitted on read-only DAG");
  }

  @Override
  public <T extends Module> T addModule(String name, Class<T> moduleClass)
  {
    throw new RuntimeException("addModule not permitted");
  }

  @Override
  public <T extends Module> T addModule(String name, T module)
  {
    throw new RuntimeException("addModule not pemitted");
  }

  @Override
  public StreamMeta addStream(String id)
  {
    throw new RuntimeException("add stream not permitted");
  }

  @Override
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T>[] sinks)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> StreamMeta addStream(String id, Operator.OutputPort<? extends T> source, Operator.InputPort<? super T> sink1, Operator.InputPort<? super T> sink2)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setAttribute(Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setAttribute(Operator operator, Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setOperatorAttribute(Operator operator, Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setOutputPortAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setUnifierAttribute(Operator.OutputPort<?> port, Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public <T> void setInputPortAttribute(Operator.InputPort<?> port, Attribute<T> key, T value)
  {
    throw new RuntimeException("not permitted");
  }

  @Override
  public OperatorMeta getOperatorMeta(String operatorId)
  {
    return parent.getOperatorMeta(operatorId);
  }

  @Override
  public OperatorMeta getMeta(Operator operator)
  {
    return parent.getMeta(operator);
  }

  @Override
  public <T extends OperatorMeta> Collection<T> getAllOperatorsMeta()
  {
    return (Collection<T>)parent.getAllOperatorsMeta();
  }

  @Override
  public <T extends OperatorMeta> Collection<T> getRootOperatorsMeta()
  {
    return (Collection<T>)parent.getRootOperatorsMeta();
  }

  @Override
  public <T extends StreamMeta> Collection<T> getAllStreamsMeta()
  {
    return (Collection<T>)parent.getAllStreamsMeta();
  }
}
