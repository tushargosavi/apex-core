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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class ThreadLocalValidator implements ComponentValidator<LogicalPlan>
{
  private Map<LogicalPlan.OperatorMeta, Integer> oioRootMap = new HashMap<>();
  private List<ValidationIssue> issues = new ArrayList<>();

  @Override
  public Collection<? extends ValidationIssue> validate(LogicalPlan dag)
  {
    for (LogicalPlan.OperatorMeta om : dag.getAllOperators()) {
      for (LogicalPlan.StreamMeta sm : om.getInputStreams().values()) {
        DAG.Locality locality = sm.getLocality();
        if (locality == DAG.Locality.THREAD_LOCAL) {
          if (om.getInputStreams().size() > 1) {
            validateThreadLocal(om);
          }
        }
      }
    }
    return issues;
  }

  /**
   * Helper method for validateThreadLocal method, runs recursively
   * For a given operator, visits all upstream operators in DFS, validates and marks them as visited
   * returns hashcode of owner oio node if it exists, else returns hashcode of the supplied node
   */
  private Integer getOioRoot(LogicalPlan.OperatorMeta om)
  {
    int oioRoot;

    // operators which were already marked a visited
    if (oioRootMap.containsKey(om)) {
      return oioRootMap.get(om);
    }

    // operators which were not visited before
    switch (om.getInputStreams().size()) {
      case 1:
        LogicalPlan.StreamMeta sm = om.getInputStreams().values().stream().findFirst().get();
        if (sm.getLocality() == DAG.Locality.THREAD_LOCAL) {
          oioRoot = getOioRoot(sm.getSource().getOperatorMeta());
          oioRootMap.put(om, oioRoot);
        } else {
          oioRootMap.put(om, om.hashCode());
        }
        break;
      case 0:
        oioRootMap.put(om, om.hashCode());
        break;
      default:
        validateThreadLocal(om);
    }

    return oioRootMap.get(om);
  }

  private void validateThreadLocal(LogicalPlan.OperatorMeta om)
  {
  /*
   * Validates OIO constraints for operators with more than one input streams
   * For a node to be OIO,
   *  1. all its input streams should be OIO
   *  2. all its input streams should have OIO from single source node
   */
    // already visited and validated
    if (oioRootMap.get(om) != null) {
      return;
    }

    if (om.getOperator() instanceof Operator.DelayOperator) {
      String msg = String.format("Locality %s invalid for delay operator %s", DAG.Locality.THREAD_LOCAL, om);
      issues.add(new ValidationIssue(om, "THREAD_LOCAL", msg));
    }

    for (LogicalPlan.StreamMeta sm: om.getInputStreams().values()) {
      // validation fail as each input stream should be OIO
      if (sm.getLocality() != Locality.THREAD_LOCAL) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not %s",
            Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        issues.add(new ValidationIssue(om, "THREAD_LOCAL", msg));
      }

      if (sm.getSource().getOperatorMeta().getOperator() instanceof Operator.DelayOperator) {
        String msg = String.format("Locality %s invalid for delay operator %s", DAG.Locality.THREAD_LOCAL, sm.getSource().getOperatorMeta());
        issues.add(new ValidationIssue(om, "THREAD_LOCAL", msg));
      }

      // gets oio root for input operator for the stream
      Integer oioStreamRoot = getOioRoot(sm.getSource().getOperatorMeta());
      Integer oioRoot = oioRootMap.get(om);
      // validation fail as each input stream should have a common OIO root
      if (oioRoot != null && oioStreamRoot != oioRoot) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as at least one of the input streams is not originating from common OIO owner node",
            Locality.THREAD_LOCAL, om, Locality.THREAD_LOCAL);
        issues.add(new ValidationIssue(om, "THREAD_LOCAL", msg));
      }

      // populate oioRoot with root OIO node id for first stream, then validate for subsequent streams to have same root OIO node
      if (oioRoot == null) {
        oioRoot = oioStreamRoot;
      } else if (oioRoot.intValue() != oioStreamRoot.intValue()) {
        String msg = String.format("Locality %s invalid for operator %s with multiple input streams as they origin from different owner OIO operators", sm.getLocality(), om);
        issues.add(new ValidationIssue(om, "THREAD_LOCAL", msg));
      }
    }
  }
}
