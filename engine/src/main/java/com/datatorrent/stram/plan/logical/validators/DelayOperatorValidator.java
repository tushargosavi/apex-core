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
package com.datatorrent.stram.plan.logical.validators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

import static com.datatorrent.stram.plan.logical.LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR;

public class DelayOperatorValidator implements ComponentValidator<LogicalPlan>
{

  private static final Logger LOG = LoggerFactory.getLogger(DelayOperatorValidator.class);

  public void findInvalidDelays(LogicalPlan.OperatorMeta om, List<List<String>> invalidDelays, Stack<LogicalPlan.OperatorMeta> stack)
  {
    stack.push(om);

    // depth first successors traversal
    boolean isDelayOperator = om.getOperator() instanceof Operator.DelayOperator;
    if (isDelayOperator) {
      if (om.getValue(OperatorContext.APPLICATION_WINDOW_COUNT) != 1) {
        LOG.debug("detected DelayOperator having APPLICATION_WINDOW_COUNT not equal to 1");
        invalidDelays.add(Collections.singletonList(om.getName()));
      }
    }

    for (LogicalPlan.StreamMeta downStream: om.getOutputStreams().values()) {
      for (LogicalPlan.InputPortMeta sink : downStream.getSinks()) {
        LogicalPlan.OperatorMeta successor = sink.getOperatorMeta();
        if (isDelayOperator) {
          sink.getAttributes().put(IS_CONNECTED_TO_DELAY_OPERATOR, true);
          // Check whether all downstream operators are already visited in the path
          if (successor != null && !stack.contains(successor)) {
            LOG.debug("detected DelayOperator does not immediately output to a visited operator {}.{}->{}.{}",
                om.getName(), downStream.getSource().getPortName(), successor.getName(), sink.getPortName());
            invalidDelays.add(Arrays.asList(om.getName(), successor.getName()));
          }
        } else {
          findInvalidDelays(successor, invalidDelays, stack);
        }
      }
    }
    stack.pop();
  }

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan dag)
  {
    List<ValidationIssue> issues = new ArrayList<>();
    List<List<String>> invalidDelays = new ArrayList<>();
    for (LogicalPlan.OperatorMeta n : dag.getRootOperators()) {
      findInvalidDelays(n, invalidDelays, new Stack<>());
    }
    if (!invalidDelays.isEmpty()) {
      issues.add(new ValidationIssue<>(dag, "INVALID_DELAYS", "Invalid delays in graph: " + invalidDelays));
    }
    return issues;
  }
}
