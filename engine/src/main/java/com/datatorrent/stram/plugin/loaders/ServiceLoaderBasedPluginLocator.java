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
package com.datatorrent.stram.plugin.loaders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.api.plugin.ApexPlugin;
import com.datatorrent.stram.api.plugin.PluginLocator;

public class ServiceLoaderBasedPluginLocator implements PluginLocator
{
  private static final Logger LOG = LoggerFactory.getLogger(ServiceLoaderBasedPluginLocator.class);

  @Override
  public Collection<ApexPlugin> discoverPlugins()
  {
    List<ApexPlugin> discovered = new ArrayList<>();
    LOG.info("detecting plugins by {} locator", this.getClass().getName());
    ServiceLoader<ApexPlugin> loader = ServiceLoader.load(ApexPlugin.class);
    for (ApexPlugin plugin : loader) {
      LOG.info("found plugin {}", plugin);
      discovered.add(plugin);
    }
    LOG.info("detected loader {} {} plugins ",this.getClass().getName(), discovered.size());
    return discovered;
  }
}
