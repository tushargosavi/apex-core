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
import com.datatorrent.api.DAG.OutputPortMeta;
import com.datatorrent.api.Operator;

public class ReadOnlyOutputPortMeta implements DAG.OutputPortMeta
{
  private final OperatorMeta ometa;
  private final OutputPortMeta opm;
  private final ReadOnlyAttributeMap attrs;
  private final OperatorMeta unifierMeta;

  public ReadOnlyOutputPortMeta(OperatorMeta ometa, OutputPortMeta opm)
  {
    this.ometa = ometa;
    this.opm = opm;
    this.attrs = new ReadOnlyAttributeMap(opm.getAttributes());
    unifierMeta = new ReadOnlyOperatorMeta(opm.getUnifierMeta());
  }

  @Override
  public Attribute.AttributeMap getAttributes()
  {
    return attrs;
  }

  @Override
  public DAG.OperatorMeta getUnifierMeta()
  {
    return unifierMeta;
  }

  @Override
  public Operator.OutputPort<?> getPort()
  {
    return opm.getPort();
  }

  @Override
  public OperatorMeta getOperatorMeta()
  {
    return ometa;
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
}
