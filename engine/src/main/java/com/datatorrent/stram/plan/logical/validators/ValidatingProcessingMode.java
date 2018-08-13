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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class ValidatingProcessingMode implements ComponentValidator<LogicalPlan>
{
  Logger LOG = LoggerFactory.getLogger(ValidatingProcessingMode.class);

  private void validateProcessingMode(LogicalPlan.OperatorMeta om, Set<LogicalPlan.OperatorMeta> visited,
      List<ValidationIssue> issues)
  {
    for (LogicalPlan.StreamMeta is : om.getInputStreams().values()) {
      if (!visited.contains(is.getSource().getOperatorMeta())) {
        // process all inputs first
        return;
      }
    }
    visited.add(om);
    Operator.ProcessingMode pm = om.getValue(OperatorContext.PROCESSING_MODE);
    for (LogicalPlan.StreamMeta os : om.getOutputStreams().values()) {
      for (LogicalPlan.InputPortMeta sink: os.getSinks()) {
        LogicalPlan.OperatorMeta sinkOm = sink.getOperatorMeta();
        Operator.ProcessingMode sinkPm = sinkOm.getAttributes() == null ? null : sinkOm.getAttributes().get(OperatorContext.PROCESSING_MODE);
        if (sinkPm == null) {
          // If the source processing mode is AT_MOST_ONCE and a processing mode is not specified for the sink then
          // set it to AT_MOST_ONCE as well
          if (Operator.ProcessingMode.AT_MOST_ONCE.equals(pm)) {
            LOG.warn("Setting processing mode for operator {} to {}", sinkOm.getName(), pm);
            sinkOm.getAttributes().put(OperatorContext.PROCESSING_MODE, pm);
          } else if (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm)) {
            // If the source processing mode is EXACTLY_ONCE and a processing mode is not specified for the sink then
            // throw a validation error
            String msg = String.format("Processing mode for %s should be AT_MOST_ONCE for source %s/%s", sinkOm.getName(), om.getName(), pm);
            issues.add(new ValidationIssue<>(null, "INVALID_PROCESSING_MODE", msg));
          }
        } else {
          /*
           * If the source processing mode is AT_MOST_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           * If the source processing mode is EXACTLY_ONCE and the processing mode for the sink is not AT_MOST_ONCE throw a validation error
           */
          if ((Operator.ProcessingMode.AT_MOST_ONCE.equals(pm) && (sinkPm != pm))
              || (Operator.ProcessingMode.EXACTLY_ONCE.equals(pm) && !Operator.ProcessingMode.AT_MOST_ONCE.equals(sinkPm))) {
            String msg = String.format("Processing mode %s/%s not valid for source %s/%s", sinkOm.getName(), sinkPm, om.getName(), pm);
            issues.add(new ValidationIssue<>(null, "INVALID_PROCESSING_MODE", msg));
          }
        }
        validateProcessingMode(sinkOm, visited, issues);
      }
    }
  }

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan dag)
  {
    Set<LogicalPlan.OperatorMeta> visited = Sets.newHashSet();
    List<ValidationIssue> issues = new ArrayList<>();
    for (LogicalPlan.OperatorMeta om : dag.getRootOperators()) {
      validateProcessingMode(om, visited, issues);
    }

    return issues;
  }
}
