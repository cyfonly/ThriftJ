package com.cyfonly.thriftj.failover;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.pool.ThriftConnectionPool;
import com.cyfonly.thriftj.pool.ThriftServer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * failover 定时侦测
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class FailoverChecker {
	private final Logger logger = LoggerFactory.getLogger(FailoverChecker.class);
	
	private volatile List<ThriftServer> serverList;
	private List<ThriftServer> backupServerList;
	private FailoverStrategy<ThriftServer> failoverStrategy;
	private ThriftConnectionPool poolProvider;
	private ConnectionValidator connectionValidator;
	private ScheduledExecutorService checkExecutor;
	private int serviceLevel;
	
	public FailoverChecker(ConnectionValidator connectionValidator, FailoverStrategy<ThriftServer> failoverStrategy, int serviceLevel) {
		this.connectionValidator = connectionValidator;
		this.failoverStrategy = failoverStrategy;
		this.serviceLevel = serviceLevel;
	}
	
	public void setConnectionPool(ThriftConnectionPool poolProvider) {
		this.poolProvider = poolProvider;
	}
	
	public void startChecking() {
		if (connectionValidator != null) {
			ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Fail Check Worker").build();
			checkExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
			checkExecutor.scheduleAtFixedRate(new Checker(), 5000, 5000, TimeUnit.MILLISECONDS);
		}
	}
	
	private class Checker implements Runnable {
		@Override
		public void run() {
			for (ThriftServer thriftServer : getAvailableServers(true)) {
				TTransport tt = null;
				boolean valid = false;
				try {
					tt = poolProvider.getConnection(thriftServer);
					valid = connectionValidator.isValid(tt);
				} catch (Exception e) {
					valid = false;
					logger.warn(e.getMessage(), e);
				} finally {
					if (tt != null) {
						if (valid) {
							poolProvider.returnConnection(thriftServer, tt);
						} else {
							failoverStrategy.fail(thriftServer);
							poolProvider.returnBrokenConnection(thriftServer, tt);
						}
					} else {
						failoverStrategy.fail(thriftServer);
					}
				}
			}
		}
	}
	
	public void setServerList(List<ThriftServer> serverList) {
		this.serverList = serverList ;
	}
	
	public void setBackupServerList(List<ThriftServer> backupServerList) {
		this.backupServerList = backupServerList;
	}
	
	public List<ThriftServer> getAvailableServers() {
		return getAvailableServers(false);
	}
	
	private List<ThriftServer> getAvailableServers(boolean all) {
		List<ThriftServer> returnList = new ArrayList<>();
		Set<ThriftServer> failedServers = failoverStrategy.getFailed();
		for (ThriftServer thriftServer : serverList) {
			if (!failedServers.contains(thriftServer))
				returnList.add(thriftServer);
		}
		if (this.serviceLevel == Constant.ServiceLevel.SERVERS_ONLY) {
			return returnList;
		}
		if ((all || returnList.isEmpty()) && !backupServerList.isEmpty()) {
			for (ThriftServer thriftServer : backupServerList) {
				if (!failedServers.contains(thriftServer))
					returnList.add(thriftServer);
			}
		}
		if (this.serviceLevel == Constant.ServiceLevel.ALL_SERVERS) {
			return returnList;
		}
		if(returnList.isEmpty()){
			returnList.addAll(serverList);
		}
		return returnList;
	}
	
	public FailoverStrategy<ThriftServer> getFailoverStrategy() {
		return failoverStrategy;
	}

	public ConnectionValidator getConnectionValidator() {
		return connectionValidator;
	}
	
	public void stopChecking() {
		if (checkExecutor != null)
			checkExecutor.shutdown();
	}
}
