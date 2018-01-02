/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.yarn.plan.logical;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.datatorrent.api.Attribute;

public class LogicalPlanAttributes
{
  public static Attribute<Long> RM_TOKEN_LIFE_TIME = new Attribute<>(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);

  static {
    Attribute.AttributeMap.AttributeInitializer.initialize(LogicalPlanAttributes.class);
  }
}
