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

import com.datatorrent.stram.StramAppContext;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;

public interface PluginManager
{
  class RegistrationType<T>
  {
  }

  RegistrationType<StreamingContainerUmbilicalProtocol.ContainerHeartbeat> HEARTBEAT = new RegistrationType<>();
  RegistrationType<StramEvent> STRAM_EVENT = new RegistrationType<>();
  RegistrationType<Long> COMMIT_EVENT = new RegistrationType<>();

  <T> void register(RegistrationType<T> type, Handler<T> handler);

  interface Handler<T>
  {
    void handle(T data);
  }

  void submit(Runnable task);

  // provide application level configuration , name, id, other configurations.
  StramAppContext getApplicationContext();

  // get information about other things
  PluginContext getPluginContext();
}
