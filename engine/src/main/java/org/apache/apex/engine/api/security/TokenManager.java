/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.api.security;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Component;
import com.datatorrent.api.Context;

@InterfaceStability.Evolving
public interface TokenManager<CONTEXT extends Context> extends Component<CONTEXT>
{
  String issueToken(String username, String service) throws IOException;

  String verifyToken(String token, String remote) throws IOException;
}
