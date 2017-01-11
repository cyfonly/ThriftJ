package com.cyfonly.thriftj.test.thriftclient;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cyfonly.thriftj.ThriftClient;
import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.failover.ConnectionValidator;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.pool.ThriftServer;
import com.cyfonly.thriftj.test.thriftserver.thrift.QryResult;
import com.cyfonly.thriftj.test.thriftserver.thrift.TestThriftJ;


/**
 * TestThriftJ.thrift Client，基于 ThriftJ 组件
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ThriftClientTest {
	private static final Logger logger = LoggerFactory.getLogger(ThriftClientTest.class);
	
	private static final String servers = "127.0.0.1:10001,127.0.0.1:10002";
	
	public static void main(String[] args){
		
		ConnectionValidator validator = new ConnectionValidator() {
			@Override
			public boolean isValid(TTransport object) {
				return object.isOpen();
			}
		};
		GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
		FailoverStrategy failoverStrategy = new FailoverStrategy();
		
		final ThriftClient thriftClient = new ThriftClient();
		thriftClient.servers(servers)
					.loadBalance(Constant.LoadBalance.RANDOM)
					.connectionValidator(validator)
					.poolConfig(poolConfig)
					.failoverStrategy(failoverStrategy)
					.connTimeout(5)
					.backupServers("")
					.serviceLevel(Constant.ServiceLevel.NOT_EMPTY)
					.start();
		
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				//打印从thriftClient获取到的可用服务列表
				StringBuffer buffer = new StringBuffer();
				List<ThriftServer> servers = thriftClient.getAvailableServers();
				for(ThriftServer server : servers){
					buffer.append(server.getHost()).append(":").append(server.getPort()).append(",");
				}
				logger.info("ThriftServers:[" + (buffer.length() == 0 ? "No avaliable server" : buffer.toString().substring(0, buffer.length()-1)) + "]");
				
				if(buffer.length() > 0){
					try {
						//测试服务是否可用
						TestThriftJ.Client client = thriftClient.iface(TestThriftJ.Client.class);
						QryResult result = client.qryTest(1);
						System.out.println("result[code=" + result.code + " msg=" + result.msg + "]");
					} catch(Throwable t){
						logger.error("-------------exception happen", t);
					}
				}
			}
		}, 0, 10, TimeUnit.SECONDS);
	}

}
