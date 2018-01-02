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
package org.apache.apex.engine;

import org.apache.apex.engine.api.StreamingAppMasterContext;
import org.apache.commons.cli.CommandLine;

import com.datatorrent.stram.api.BaseContext;

public class StreamingAppMasterSetupContext extends BaseContext implements StreamingAppMasterContext
{

  CommandLine commandLine;

  // TODO: Add attributes that can be used by the app master later
  public StreamingAppMasterSetupContext(CommandLine commandLine)
  {
    super(null, null);
    this.commandLine = commandLine;
  }

  @Override
  public CommandLine getCommandLine()
  {
    return commandLine;
  }
}
