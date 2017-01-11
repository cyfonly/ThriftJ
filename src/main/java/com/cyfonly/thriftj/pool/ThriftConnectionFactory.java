package com.cyfonly.thriftj.pool;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cyfonly.thriftj.failover.ConnectionValidator;
import com.cyfonly.thriftj.failover.FailoverChecker;


/**
 * Thrift Connection 工厂类
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class ThriftConnectionFactory implements KeyedPooledObjectFactory<ThriftServer, TTransport>{
	private static final Logger logger = LoggerFactory.getLogger(ThriftConnectionFactory.class);
	
	private FailoverChecker failoverChecker;
	private int timeout;
	
	public ThriftConnectionFactory(int timeout) {
		this.timeout = timeout;
	}
	
	public ThriftConnectionFactory(FailoverChecker failoverChecker, int timeout) {
		this.failoverChecker = failoverChecker;
		this.timeout = timeout;
	}
	
	@Override
	public PooledObject<TTransport> makeObject(ThriftServer thriftServer) throws Exception {
		TSocket tsocket = new TSocket(thriftServer.getHost(), thriftServer.getPort());
		tsocket.setTimeout(timeout);
		TFramedTransport transport = new TFramedTransport(tsocket);
		
		transport.open();
		DefaultPooledObject<TTransport> result = new DefaultPooledObject<TTransport>(transport);
		logger.trace("Make new thrift connection: {}:{}", thriftServer.getHost(), thriftServer.getPort());
		
		return result;
	}
	
	@Override
	public boolean validateObject(ThriftServer thriftServer, PooledObject<TTransport> pooledObject) {
		boolean isValidate;
		try {
			if (failoverChecker == null) {
				isValidate = pooledObject.getObject().isOpen();
			} else {
				ConnectionValidator validator = failoverChecker.getConnectionValidator();
				isValidate = pooledObject.getObject().isOpen() && (validator == null || validator.isValid(pooledObject.getObject()));
			}
		} catch (Throwable e) {
			logger.warn("Fail to validate tsocket: {}:{}", new Object[]{thriftServer.getHost(), thriftServer.getPort(), e});
			isValidate = false;
		}
		if (failoverChecker != null && !isValidate) {
			failoverChecker.getFailoverStrategy().fail(thriftServer);
		}
		logger.info("ValidateObject isValidate:{}", isValidate);
		
		return isValidate;
	}

	@Override
	public void destroyObject(ThriftServer thriftServer, PooledObject<TTransport> pooledObject) throws Exception {
		TTransport transport = pooledObject.getObject();
		if (transport != null) {
			transport.close();
			logger.trace("Close thrift connection: {}:{}", thriftServer.getHost(), thriftServer.getPort());
		}
	}

	@Override
	public void activateObject(ThriftServer thriftServer, PooledObject<TTransport> pooledObject) throws Exception {
	}
	
	@Override
	public void passivateObject(ThriftServer arg0, PooledObject<TTransport> pooledObject) throws Exception {
	}
}
