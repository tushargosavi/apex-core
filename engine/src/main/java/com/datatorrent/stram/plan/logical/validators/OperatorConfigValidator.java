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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Validate operator configuration. The validate checks for the propery value set and the annotation
 * used on the property. If any property voilates the contraints then a error is returned.
 */
public class OperatorConfigValidator implements ComponentValidator<LogicalPlan.OperatorMeta>
{
  private final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
  private final Validator validator = factory.getValidator();

  @Override
  public Collection<ValidationIssue> validate(LogicalPlan.OperatorMeta om)
  {
    Set<ValidationIssue> copySet = new HashSet<>();
    Set<ConstraintViolation<Operator>> constraintViolations = validator.validate(om.getOperator());
    if (!constraintViolations.isEmpty()) {
      for (ConstraintViolation<Operator> cv : constraintViolations) {
        copySet.add(new ValidationIssue(om, cv));
      }
    }
    return copySet;
  }

  public static final String CONSTRAINT_VIOLATION = "CONSTRAINT_VIOLATION";

  public static class ValidationIssue extends ComponentValidator.ValidationIssue<LogicalPlan.OperatorMeta>
  {
    ConstraintViolation<Operator> cv;

    ValidationIssue(LogicalPlan.OperatorMeta om, ConstraintViolation<Operator> cv)
    {
      super(om, CONSTRAINT_VIOLATION, cv.toString());
      this.cv = cv;
    }

    @Override
    public String toString()
    {
      return super.toString();
    }
  }
}
