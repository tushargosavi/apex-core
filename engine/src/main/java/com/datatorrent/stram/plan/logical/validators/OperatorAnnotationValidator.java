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

import com.datatorrent.api.Partitioner;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.PortContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class OperatorAnnotationValidator implements ComponentValidator<LogicalPlan.OperatorMeta>
{
  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan.OperatorMeta om)
  {
    List<ValidationIssue<LogicalPlan.OperatorMeta>> issues = new ArrayList<>();
    if (om.getOperatorAnnotation() == null) {
      return issues;
    }

    for (LogicalPlan.InputPortMeta pm : om.getInputStreams().keySet()) {
      Boolean paralellPartition = pm.getValue(PortContext.PARTITION_PARALLEL);
      if (paralellPartition) {
        issues.add(new ValidationIssue<>(om, "PARTITION_ANNOTATION",
            "Operator " + om.getName() + " is not partitionable but PARTITION_PARALLEL attribute is set"));
      }
    }

    // Check if the operator implements Partitioner
    if (om.getValue(OperatorContext.PARTITIONER) != null ||
        om.getAttributes() != null && !om.getAttributes().contains(OperatorContext.PARTITIONER) && Partitioner.class.isAssignableFrom(om.getOperator().getClass())) {
      issues.add(new ValidationIssue<>(om, "INVALID_PARTITIONER",
          "Operator " + om.getName() + " provides partitioning capabilities but the annotation on the" +
              "operator class declares it non partitionable!"));
    }
    return issues;
  }
}
