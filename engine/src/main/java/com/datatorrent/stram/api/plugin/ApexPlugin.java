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
package com.datatorrent.stram.api.plugin;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * An Apex plugin is a user code which runs inside Stram. The interaction
 * between plugin and Stram is managed by PluginManager. Plugin can register to handle event in interest
 * with callback handler using ${@link PluginManager#register(com.datatorrent.stram.api.plugin.PluginManager.RegistrationType, com.datatorrent.stram.api.plugin.PluginManager.Handler)}
 *
 * Following events are supported
 * <ul>
 *   <li>{@see PluginManager.HEARTBEAT} The heartbeat from a container is delivered to the plugin after it has been handled by stram</li>
 *   <li>{@see PluginManager.STRAM_EVENT} All the Stram event generated in Stram will be delivered to the plugin</li>
 *   <li>{@see PluginManager.COMMIT_EVENT} When committedWindowId changes in the platform an event will be delivered to the plugin</li>
 * </ul>
 *
 * A plugin can register for multiple handler for same event, they will be call one after another in the same
 * order in which they were registered. All the registered callback are guaranteed to be called from a single thread.
 * Plugin should cleanup additional resources created by it during shutdown such as helper threads and open files.
 */
@InterfaceStability.Evolving
public interface ApexPlugin
{
  /**
   * An Initialization method for ApexPlugin. platform will be provide implementation for PluginManager.  plugin can registered
   * for interested event using ${@link PluginManager#register(com.datatorrent.stram.api.plugin.PluginManager.RegistrationType, com.datatorrent.stram.api.plugin.PluginManager.Handler)}
   * The plugin can initialize/setup other external resources required for execution.
   * @param manager an instance of PluginManager.
   */
  void init(PluginManager manager);

  /**
   * Platform will call this method during shutdown of an application. Plugin should release any resources held by it
   * during shutdown. If plugin has created helper threads then they should wait till the treads are finished.
   */
  void shutdown();

}
