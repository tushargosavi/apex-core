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
import javax.validation.ValidationException;

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
  Map<String, DAG.StreamMeta> changedStreams = Maps.newHashMap();

  public DAGChangeTransactionImpl(LogicalPlan plan, int version)
  {
    parent = plan;
    this.transactionId = version;
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

  public <T> void setAttribute(Attribute<T> key, T value)
  {
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
    if (parent.operators.containsValue(operator)) {
      throw new IllegalArgumentException("Trying to modify existing operator " + operator);
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
      if (plan.getOperatorMeta(ometa.getName()) != null) {
        throw new ValidationException("Operator already exists in Original DAG");
      }
      plan.addOperator(ometa);
      newOperators.put(ometa.getName(), ometa);
    }

    /**
     * Merge streams in the plan
     */
    for (DAG.StreamMeta smeta : getAllStreams()) {
      DAG.StreamMeta original = plan.getStream(smeta.getName());

      // Only adding new sink is supported
      if (original != null) {
        if (smeta.getLocality() != original.getLocality()) {
          throw new ValidationException("Can not change locality for existing stream ");
        }

        if (smeta.getSource() != null) {
          throw new ValidationException("Can not specify new source for existing stream");
        }

        for (DAG.InputPortMeta ipm : smeta.getSinks()) {
          original.addSink(ipm.getPort());
        }
        changedStreams.put(original.getName(), original);
      } else {
        // add stream to new plan
        plan.addStream(smeta);
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
  }

  @Override
  public int getTransactionId()
  {
    return transactionId;
  }
}
