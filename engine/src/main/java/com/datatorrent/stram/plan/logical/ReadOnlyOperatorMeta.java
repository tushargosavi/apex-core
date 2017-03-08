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
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.InputPortMeta;
import com.datatorrent.api.DAG.StreamMeta;
import com.datatorrent.api.Operator;

/**
 * Wrap existing operator meta, do not allow to modify the attributes of the
 * operator.
 */
public class ReadOnlyOperatorMeta implements DAG.OperatorMeta
{
  private final DAG.OperatorMeta ometa;
  private final ReadOnlyAttributeMap attrs;
  private final Map<Operator.InputPort<?>, ReadOnlyInputPortMeta> inputs = Maps.newHashMap();
  private final Map<Operator.OutputPort<?>, ReadOnlyOutputPortMeta> outputs = Maps.newHashMap();
  private final LinkedHashMap<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> inputStreams = new LinkedHashMap<>();
  private final LinkedHashMap<LogicalPlan.OutputPortMeta, LogicalPlan.StreamMeta> outputStreams = new LinkedHashMap<>();

  public ReadOnlyOperatorMeta(DAG.OperatorMeta ometa)
  {
    this.ometa = ometa;
    this.attrs = new ReadOnlyAttributeMap(ometa.getAttributes());
    Map<DAG.InputPortMeta, DAG.StreamMeta> streams = ometa.getInputStreams();
    for (Map.Entry<InputPortMeta, StreamMeta> entry : streams.entrySet()) {
      inputs.put(entry.getKey().getPort(), new ExtendableStreamMeta(entry.getValue()));
    }
  }

  @Override
  public synchronized Attribute.AttributeMap getAttributes()
  {
    return attrs;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    return attrs.get(key);
  }

  @Override
  public void setCounters(Object counters)
  {

  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {

  }

  @Override
  public String getName()
  {
    return ometa.getName();
  }

  @Override
  public Operator getOperator()
  {
    return ometa.getOperator();
  }

  @Override
  public DAG.InputPortMeta getMeta(Operator.InputPort<?> port)
  {
    DAG.InputPortMeta ipm = inputs.get(port);
    if (ipm == null) {
      ipm = new ReadOnlyInputPortMeta(this, ometa.getMeta(port));
      inputs.put(port, (ReadOnlyInputPortMeta)ipm);
    }
    return ipm;
  }

  @Override
  public DAG.OutputPortMeta getMeta(Operator.OutputPort<?> port)
  {
    return ometa.getMeta(port);
  }

  @Override
  public <K extends DAG.InputPortMeta, V extends DAG.StreamMeta> Map<K, V> getInputStreams()
  {
    return ometa.getInputStreams();
  }

  @Override
  public <K extends DAG.OutputPortMeta, V extends DAG.StreamMeta> Map<K, V> getOutputStreams()
  {
    return ometa.getOutputStreams();
  }
}
