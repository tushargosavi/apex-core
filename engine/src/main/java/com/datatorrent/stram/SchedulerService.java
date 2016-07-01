package com.datatorrent.stram;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol;

public class SchedulerService extends AbstractService
{
  private static final Logger LOG = LoggerFactory.getLogger(StreamingContainerParent.class);
  private final SecretManager<? extends TokenIdentifier> secretManager;
  private RPC.Server server;
  private InetSocketAddress address;

  public SchedulerService(String name, StreamingContainerManager dnodeMgr, SecretManager<? extends TokenIdentifier> secretManager)
  {
    super(name);
    this.secretManager = secretManager;
  }

  @Override
  public void init(Configuration conf)
  {
    super.init(conf);
  }

  @Override
  public void start()
  {
    startRpcServer();
    super.start();
  }

  @Override
  public void stop()
  {
    super.stop();
  }

  protected void startRpcServer()
  {
    Configuration conf = getConfig();
    LOG.info("Config: " + conf);
    try {
      server = new RPC.Builder(conf).setProtocol(StreamingContainerUmbilicalProtocol.class).setInstance(this)
        .setBindAddress("0.0.0.0").setPort(0).setNumHandlers(1).setSecretManager(secretManager)
        .setVerbose(false).build();

      // Enable service authorization?
      if (conf.getBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, false)) {
        //refreshServiceAcls(conf, new MRAMPolicyProvider());
        server.refreshServiceAcl(conf, new PolicyProvider()
        {

          @Override
          public Service[] getServices()
          {
            return (new Service[]{
              new Service(StreamingContainerUmbilicalProtocol.class
                .getName(), StreamingContainerUmbilicalProtocol.class)
            });
          }
        });
      }

      server.start();
      this.address = NetUtils.getConnectAddress(server);
      LOG.info("Schedular service started at " + this.address);
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }
  }

}
