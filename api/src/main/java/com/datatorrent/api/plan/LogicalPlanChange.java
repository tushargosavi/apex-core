package com.datatorrent.api.plan;

public interface LogicalPlanChange
{
  public void execute(PlanModifier pm);
}
