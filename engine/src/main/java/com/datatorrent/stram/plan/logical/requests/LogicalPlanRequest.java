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
package com.datatorrent.stram.plan.logical.requests;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;

import com.datatorrent.api.DAG;
import com.datatorrent.api.plan.LogicalPlanChange;
import com.datatorrent.api.plan.PlanModifier;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * <p>Abstract LogicalPlanRequest class.</p>
 *
 * @since 0.3.2
 */
public abstract class LogicalPlanRequest implements LogicalPlanChange
{
  public String getRequestType()
  {
    return this.getClass().getSimpleName();
  }

  @Override
  public String toString()
  {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (IOException ex) {
      return ex.toString();
    }
  }

  public abstract void execute(PlanModifier pm);

  public static DAG getDag() {
    return new LogicalPlan();
  }
}
