/**
 * Copyright (c) 2012-2017 DataTorrent, Inc.
 * ALL Rights Reserved.
 */
package org.apache.apex.engine.security.delegation;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.api.security.TokenManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Throwables;

import com.datatorrent.api.Context;
import com.datatorrent.stram.security.StramDelegationTokenIdentifier;
import com.datatorrent.stram.security.StramDelegationTokenManager;

public class DelegationTokenManager implements TokenManager
{
  private static final Logger logger = LoggerFactory.getLogger(DelegationTokenManager.class);

  StramDelegationTokenManager tokenManager;

  String loginUser;

  public DelegationTokenManager(StramDelegationTokenManager tokenManager)
  {
    this.tokenManager = tokenManager;
  }

  @Override
  public String issueToken(String username, String service) throws IOException
  {
    StramDelegationTokenIdentifier tokenIdentifier = new StramDelegationTokenIdentifier(new Text(username), new Text(loginUser), new Text());
    //tokenIdentifier.setSequenceNumber(sequenceNumber.getAndAdd(1));
    //byte[] password = tokenManager.addIdentifier(tokenIdentifier);
    //Token<StramDelegationTokenIdentifier> token = new Token<StramDelegationTokenIdentifier>(tokenIdentifier.getBytes(), password, tokenIdentifier.getKind(), new Text(service));
    Token<StramDelegationTokenIdentifier> token = new Token<>(tokenIdentifier, tokenManager);
    token.setService(new Text(service));
    return token.encodeToUrlString();
  }

  @Override
  public String verifyToken(String token, String remote) throws IOException
  {
    Token<StramDelegationTokenIdentifier> stramToken = new Token<>();
    try {
      stramToken.decodeFromUrlString(token);
    } catch (IOException e) {
      logger.debug("{}: error decoding token: {}", remote, e.getMessage());
      return null;
    }
    byte[] identifier = stramToken.getIdentifier();
    byte[] password = stramToken.getPassword();
    StramDelegationTokenIdentifier tokenIdentifier = new StramDelegationTokenIdentifier();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(identifier));
    try {
      tokenIdentifier.readFields(input);
    } catch (IOException e) {
      logger.debug("{}: error decoding identifier: {}", remote, e.getMessage());
      return null;
    }
    try {
      tokenManager.verifyToken(tokenIdentifier, password);
    } catch (SecretManager.InvalidToken e) {
      logger.debug("{}: invalid token {}: {}", remote, tokenIdentifier, e.getMessage());
      return null;
    }
    return tokenIdentifier.getOwner().toString();
  }

  @Override
  public void setup(Context context)
  {
    try {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      if (ugi != null) {
        loginUser = ugi.getUserName();
      }
      tokenManager.startThreads();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void teardown()
  {
    tokenManager.stopThreads();
  }
}
