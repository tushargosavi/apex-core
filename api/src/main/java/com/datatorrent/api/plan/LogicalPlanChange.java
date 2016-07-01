package com.datatorrent.api.plan;

import com.datatorrent.api.DAG;

public interface LogicalPlanChange
{
  public DAG createDAG();
  public void execute(PlanModifier pm);
}
