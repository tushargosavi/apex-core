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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import com.datatorrent.api.Operator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;

public class CycleDetector
{
  public static class ValidationContext
  {
    public int nodeIndex = 0;
    public Stack<OperatorMeta> stack = new Stack<>();
    public Stack<OperatorMeta> path = new Stack<>();
    public List<Set<OperatorMeta>> stronglyConnected = new ArrayList<>();
    public OperatorMeta invalidLoopAt;
    public List<Set<OperatorMeta>> invalidCycles = new ArrayList<>();
  }

   /**
   * Check for cycles in the graph reachable from start node n. This is done by
   * attempting to find strongly connected components.
   *
   * @see <a href="http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm">http://en.wikipedia.org/wiki/Tarjan%E2%80%99s_strongly_connected_components_algorithm</a>
   *
   * @param om
   * @param ctx
   */
  public void findStronglyConnected(LogicalPlan.OperatorMeta om, ValidationContext ctx)
  {
    om.setNindex(ctx.nodeIndex);
    om.setLowlink(ctx.nodeIndex);
    ctx.nodeIndex++;
    ctx.stack.push(om);
    ctx.path.push(om);

    // depth first successors traversal
    for (LogicalPlan.StreamMeta downStream: om.getOutputStreams().values()) {
      for (LogicalPlan.InputPortMeta sink: downStream.getSinks()) {
        LogicalPlan.OperatorMeta successor = sink.getOperatorMeta();
        if (successor == null) {
          continue;
        }
        // check for self referencing node
        if (om == successor) {
          ctx.invalidCycles.add(Collections.singleton(om));
        }
        if (successor.getNindex() == null) {
          // not visited yet
          findStronglyConnected(successor, ctx);
          om.setLowlink(Math.min(om.getLowlink(), successor.getLowlink()));
        } else if (ctx.stack.contains(successor)) {
          om.setLowlink(Math.min(om.getLowlink(), successor.getNindex()));
          boolean isDelayLoop = false;
          for (int i = ctx.stack.size(); i > 0; i--) {
            LogicalPlan.OperatorMeta om2 = ctx.stack.get(i - 1);
            if (om2.getOperator() instanceof Operator.DelayOperator) {
              isDelayLoop = true;
            }
            if (om2 == successor) {
              break;
            }
          }
          if (!isDelayLoop) {

            ctx.invalidLoopAt = successor;
          }
        }
      }
    }

    // pop stack for all root operators
    if (om.getLowlink().equals(om.getNindex())) {
      Set<LogicalPlan.OperatorMeta> connectedSet = new LinkedHashSet<>(ctx.stack.size());
      while (!ctx.stack.isEmpty()) {
        LogicalPlan.OperatorMeta n2 = ctx.stack.pop();
        connectedSet.add(n2);
        if (n2 == om) {
          break; // collected all connected operators
        }
      }
      // strongly connected (cycle) if more than one node in stack
      if (connectedSet.size() > 1) {
        ctx.stronglyConnected.add(connectedSet);
        if (connectedSet.contains(ctx.invalidLoopAt)) {
          ctx.invalidCycles.add(connectedSet);
        }
      }
    }
    ctx.path.pop();
  }

  public ValidationContext findStronglyConnected(LogicalPlan dag)
  {
    ValidationContext ctx = new ValidationContext();
    for (OperatorMeta om: dag.getAllOperators()) {
      if (om.getNindex() != null) {
        findStronglyConnected(om, ctx);
      }
    }
    return ctx;
  }
}
