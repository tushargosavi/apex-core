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

public class InputPortValidator implements ComponentValidator<LogicalPlan.InputPortMeta>
{
  private final LogicalPlan.OperatorMeta om;

  public InputPortValidator(LogicalPlan.OperatorMeta om)
  {
    this.om = om;
  }

  public static final String REQUIRED_PORT_CONNECTION = "REQUIRED_PORT_CONNECTION";

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan.InputPortMeta pm)
  {
    List<ValidationIssue> issues = new ArrayList<>();
    AttributeSerializationValidator.validateStatic(pm.getAttributes(), om.getName() + "." + pm.getPortName());
    LogicalPlan.StreamMeta sm = om.getInputStreams().get(pm);
    if (sm == null) {
      if (!pm.isOptional()) {
        issues.add(new ValidationIssue<>(pm, "PORT_CONNECTION_REQQUIRED",
            "Input port connection required: " + om.getName() + "." + pm.getPortName()));
      }
    } else {
      if (pm.isPortHidden()) {
        String msg = String.format("Invalid port connected: %s.%s is hidden by %s.%s", pm.getHidingOperatorClassName(),
            pm.getPortName(), pm.getOperatorMeta().getOperator().getClass().getName(), pm.getPortName());
        issues.add(new ValidationIssue<>(pm, "HIDDEN_PORT", msg));
      }
    }
    return issues;
  }
}
