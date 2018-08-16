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

public class OperatorPortValidations implements ComponentValidator<LogicalPlan.OperatorMeta>
{
  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan.OperatorMeta om)
  {
    InputPortValidator inputPortValidator = new InputPortValidator(om);
    OutputPortValidator outputPortValidator = new OutputPortValidator(om);

    LogicalPlan.OperatorMeta.PortMapping pm = om.getPortMapping();

    List<ValidationIssue> issues = new ArrayList<>();
    for (LogicalPlan.InputPortMeta ipm : pm.inPortMap.values()) {
      issues.addAll(inputPortValidator.validate(ipm));
    }

    for (LogicalPlan.OutputPortMeta opm : pm.outPortMap.values()) {
      issues.addAll(outputPortValidator.validate(opm));
    }
    return issues;
  }
}
