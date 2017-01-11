package com.cyfonly.thriftj.loadbalance;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.exceptions.NoServerAvailableException;
import com.cyfonly.thriftj.exceptions.ValidationException;
import com.cyfonly.thriftj.failover.ConnectionValidator;
import com.cyfonly.thriftj.failover.FailoverChecker;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.pool.DefaultThriftConnectionPool;
import com.cyfonly.thriftj.pool.ThriftConnectionFactory;
import com.cyfonly.thriftj.pool.ThriftServer;
import com.cyfonly.thriftj.utils.MurmurHash3;
import com.cyfonly.thriftj.utils.ThriftClientUtil;
import com.google.common.base.Charsets;


/**
 * 基于 load balance 的 Client 选择器
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ClientSelector {
	
	private FailoverChecker failoverChecker;
    private DefaultThriftConnectionPool poolProvider;
    private int loadBalance;
    
    private AtomicInteger i = new AtomicInteger(0);
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public ClientSelector(String servers, int loadBalance, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy, int connTimeout, String backupServers) {
    	this.failoverChecker = new FailoverChecker(validator, strategy);
    	this.poolProvider = new DefaultThriftConnectionPool(new ThriftConnectionFactory(failoverChecker, connTimeout), poolConfig);
    	failoverChecker.setConnectionPool(poolProvider);
    	failoverChecker.setServerList(ThriftServer.parse(servers));
        if (StringUtils.isNotEmpty(backupServers)) {
        	failoverChecker.setBackupServerList(ThriftServer.parse(backupServers));
        } else{
        	failoverChecker.setBackupServerList(new ArrayList<ThriftServer>());
        }
        failoverChecker.startChecking();
    }
	
	public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
		if (this.loadBalance == Constant.LoadBalance.HASH) {
			throw new ValidationException("Can not use HASH without a key.");
		}
		
		switch (this.loadBalance) {
		case Constant.LoadBalance.RANDOM:
			return getRandomClient(ifaceClass);
		case Constant.LoadBalance.ROUND_ROBIN:
			return getRRClient(ifaceClass);
		case Constant.LoadBalance.WEIGHT:
			return getWeightClient(ifaceClass);
		default:
			return getRandomClient(ifaceClass);
		}
    }
	
	public <X extends TServiceClient> X iface(Class<X> ifaceClass, String key) {
		if (this.loadBalance != Constant.LoadBalance.HASH) {
			throw new ValidationException("Must use other load balance strategy.");
		}
		
		return getHashIface(ifaceClass, key);
    }
	
	protected <X extends TServiceClient> X getRandomClient(Class<X> ifaceClass) {
		return iface(ifaceClass, ThriftClientUtil.randomNextInt());
	}
	
	protected <X extends TServiceClient> X getRRClient(Class<X> ifaceClass) {
		return iface(ifaceClass, i.getAndDecrement());
	}
	
	protected <X extends TServiceClient> X getWeightClient(Class<X> ifaceClass) {
		List<ThriftServer> servers = getAvaliableServers();
		if (servers == null || servers.isEmpty()) {
			throw new NoServerAvailableException("No server available.");
		}
		int[] weights = new int[servers.size()];
		for (int i = 0; i < servers.size(); i++) {
			weights[i] = servers.get(i).getWeight();
		}
		return iface(ifaceClass, servers.get(ThriftClientUtil.chooseWithWeight(weights)));
	}
	
	protected <X extends TServiceClient> X getHashIface(Class<X> ifaceClass, String key) {
		byte[] bytes = key.getBytes(Charsets.UTF_8);
        return iface(ifaceClass, MurmurHash3.murmurhash3_x86_32(bytes, 0, bytes.length, 0x1234ABCD));
	}
	
	protected <X extends TServiceClient> X iface(Class<X> ifaceClass, int index) {
        List<ThriftServer> serverList = getAvaliableServers();
        if (serverList == null || serverList.isEmpty()) {
            throw new NoServerAvailableException("No server available.");
        }
        index = Math.abs(index);
        final ThriftServer selected = serverList.get(index % serverList.size());
        return iface(ifaceClass, selected);
    }
	
	@SuppressWarnings("unchecked")
	protected <X extends TServiceClient> X iface(final Class<X> ifaceClass, final ThriftServer selected) {
        final TTransport transport;
        try {
            transport = poolProvider.getConnection(selected);
        } catch (RuntimeException e) {
            if (e.getCause() != null && e.getCause() instanceof TTransportException) {
            	failoverChecker.getFailoverStrategy().fail(selected);
            }
            throw e;
        }
        TProtocol protocol = new TBinaryProtocol(transport);

        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(ifaceClass);
        factory.setFilter(new MethodFilter() {
            @Override
            public boolean isHandled(Method m) {
                return ThriftClientUtil.getInterfaceMethodNames(ifaceClass).contains(m.getName());
            }
        });
        try {
            X x = (X) factory.create(new Class[]{TProtocol.class}, new Object[]{protocol});
            ((Proxy) x).setHandler(new MethodHandler() {
                @Override
                public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                    boolean success = false;
                    try {
                        Object result = proceed.invoke(self, args);
                        success = true;
                        return result;
                    } finally {
                        if (success) {
                            poolProvider.returnConnection(selected, transport);
                        } else {
                            failoverChecker.getFailoverStrategy().fail(selected);
                            poolProvider.returnBrokenConnection(selected, transport);
                        }
                    }
                }
            });
            return x;
        } catch (Exception e) {
            throw new RuntimeException("Fail to create proxy.", e);
        }
    }
	
	public List<ThriftServer> getAvaliableServers() {
        return failoverChecker.getAvailableServers();
    }
	
	public void close(){
		failoverChecker.stopChecking();
        poolProvider.close();
	}
}
