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
package com.datatorrent.stram.plan.logical.visitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.datatorrent.api.DAG;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;

public abstract class DAGOrderDFSTraversal implements DAGTraverser
{
  private final LogicalPlan dag;
  private final DAGVisitor visitor;
  private Set<OperatorMeta> visitedOperators = new HashSet<>();
  private Set<Stream> visitedStreams = new HashSet<>();

  private void test()
  {
    Stack<OperatorMeta> pendingNodes = new Stack<>();
    for (OperatorMeta n : dag.getAllOperators()) {
      pendingNodes.push(n);
    }

    while (!pendingNodes.isEmpty()) {
      OperatorMeta n = pendingNodes.pop();

      if (visitedOperators.contains(n)) {
        continue;
      }

      boolean upstreamDeployed = true;

      for (Map.Entry<LogicalPlan.InputPortMeta, LogicalPlan.StreamMeta> entry : n.getInputStreams().entrySet()) {
        DAG.StreamMeta s = entry.getValue();
        boolean delay = entry.getKey().getValue(LogicalPlan.IS_CONNECTED_TO_DELAY_OPERATOR);
        // skip delay sources since it's going to be handled as downstream
        if (!delay && s.getSource() != null && !visitedOperators.contains(s.getSource().getOperatorMeta())) {
          pendingNodes.push(n);
          pendingNodes.push(s.getSource().getOperatorMeta());
          upstreamDeployed = false;
          break;
        }
      }

      if (upstreamDeployed) {
        visitor.visit(n);
      }
    }
  }

  public DAGOrderDFSTraversal(LogicalPlan dag, DAGVisitor visitor)
  {
    this.dag = dag;
    this.visitor = visitor;
  }

  @Override
  public DAGVisitor traverse()
  {
    return visitor;
  }

  @Override
  public DAGVisitor traverseFrom(OperatorMeta root)
  {
    return visitor;
  }
}
