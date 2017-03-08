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
import com.datatorrent.api.DAG.OperatorMeta;
import com.datatorrent.api.Operator;

public class ReadOnlyInputPortMeta implements DAG.InputPortMeta
{
  private final DAG.InputPortMeta meta;
  private final OperatorMeta operatorMeta;
  private final ReadOnlyAttributeMap attrs;

  @Override
  public Operator.InputPort<?> getPort()
  {
    return meta.getPort();
  }

  @Override
  public OperatorMeta getOperatorMeta()
  {
    return operatorMeta;
  }

  @Override
  public Attribute.AttributeMap getAttributes()
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

  public ReadOnlyInputPortMeta(OperatorMeta operatorMeta, DAG.InputPortMeta meta)
  {
    this.operatorMeta = operatorMeta;
    this.meta = meta;
    this.attrs = new ReadOnlyAttributeMap(meta.getAttributes());
  }
}
