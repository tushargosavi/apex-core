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

import com.datatorrent.stram.plan.logical.LogicalPlan;

public class OutputPortValidator implements ComponentValidator<LogicalPlan.OutputPortMeta>
{
  private LogicalPlan.OperatorMeta om;

  public OutputPortValidator(LogicalPlan.OperatorMeta om)
  {
    this.om = om;
  }

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan.OutputPortMeta opm)
  {
    List<ValidationIssue> issues = new ArrayList<>();
    issues.addAll(AttributeSerializationValidator.validateStatic(opm.getAttributes(), om.getName() + "." + opm.getPortName()));
    LogicalPlan.StreamMeta sm = om.getOutputStreams().get(opm);
    if (sm == null) {
      if (!opm.isOptional()) {
        issues.add(new ValidationIssue<>(om, "OUTPUT_PORT_NOT_CONNECTED",
            "Output port connection required: " + om.getName() + "." + opm.getPortName()));
      }
    } else {
      //port is connected
      if (opm.isHidden()) {
        issues.add(new ValidationIssue<>(om, "HIDDEN_PORT_CONNECTED",
            String.format("Invalid port connected: %s.%s is hidden by %s.%s", opm.getHidingOperatorClassName(),
            opm.getPortName(), opm.getOperatorMeta().getOperator().getClass().getName(), opm.getPortName())));
      }
    }
    return issues;
  }
}
