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
package com.datatorrent.stram.extensions.api;

import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class DefaultPluginLocator implements PluginLocator
{
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginLocator.class);

  private final StramAppContext context;

  public DefaultPluginLocator(StramAppContext appContext)
  {
    this.context = appContext;
  }

  @Override
  public Collection<ApexPlugin> discoverPlugins()
  {
    Collection<Object> plugins = context.getAttributes().get(LogicalPlan.APEX_LISTENERS);
    Collection<ApexPlugin> detected = new ArrayList<>();
    if (plugins != null) {
      for (Object plugin : plugins) {
        if (plugin instanceof ApexPlugin) {
          LOG.info("found plugin {}", plugin);
          detected.add((ApexPlugin)plugin);
        }
      }
    }
    return detected;
  }
}
