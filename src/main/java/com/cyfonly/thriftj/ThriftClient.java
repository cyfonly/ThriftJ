package com.cyfonly.thriftj;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.failover.ConnectionValidator;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.loadbalance.ClientSelector;
import com.cyfonly.thriftj.pool.ThriftServer;


/**
 * 基于 Apache commons-pool2 的高可用、负载均衡 Thrift client
 * @author yunfeng.cheng
 * @create 2016-11-11
 */
@SuppressWarnings("rawtypes")
public class ThriftClient {
	
	private final static int DEFAULT_CONN_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(5);
	
    private ClientSelector clientSelector;
	
    /**
     * 构造 ThriftClient，默认使用 random 的负载均衡策略
     * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
     */
	public ThriftClient(String servers) {
		this(servers, Constant.LoadBalance.RANDOM);
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 */
	public ThriftClient(String servers, int loadBalance) {
		this(servers, loadBalance, new ConnectionValidator(){
			@Override
			public boolean isValid(TTransport object) {
				return object.isOpen();
			}
		});
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 * @param validator 连接验证器
	 */
	public ThriftClient(String servers, int loadBalance, ConnectionValidator validator) {
		this(servers, loadBalance, validator, new GenericKeyedObjectPoolConfig());
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 * @param validator 连接验证器
	 * @param poolConfig 连接池配置
	 */
	public ThriftClient(String servers, int loadBalance, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig) {
		this(servers, loadBalance, validator, poolConfig, new FailoverStrategy());
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 * @param validator 连接验证器
	 * @param poolConfig 连接池配置
	 * @param strategy failover 策略
	 */
	public ThriftClient(String servers, int loadBalance, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy) {
		this(servers, loadBalance, validator, poolConfig, strategy, DEFAULT_CONN_TIMEOUT);
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 * @param validator 连接验证器
	 * @param poolConfig 连接池配置
	 * @param strategy failover 策略
	 * @param connTimeout 连接 timeout 时长，单位秒
	 */
	public ThriftClient(String servers, int loadBalance, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy, int connTimeout) {
		this(servers, loadBalance, validator, poolConfig, strategy, connTimeout, null);
	}
	
	/**
	 * 构造 ThriftClient
	 * @param servers Thrift server 列表，格式 "127.0.0.1:10001,127.0.0.1:10002"
	 * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
	 * @param validator 连接验证器
	 * @param poolConfig 连接池配置
	 * @param strategy failover 策略
	 * @param connTimeout 连接 timeout 时长，单位秒
	 * @param backupServers 备用 Thrift server，格式 "127.0.0.1:11001,127.0.0.1:11002"
	 */
	public ThriftClient(String servers, int loadBalance, ConnectionValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy, int connTimeout, String backupServers) {
        this.clientSelector = new ClientSelector(servers, loadBalance, validator, poolConfig, strategy, connTimeout, backupServers);
	}
	
	/**
	 * 根据已注册的 load balance 策略选择 TServiceClient。
	 * 本方法适用于除 HASH 外的其他负载均衡。
	 * @param ifaceClass Thrift Client
	 * @return TServiceClient
	 */
	public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
		return clientSelector.iface(ifaceClass);
	}
	
	/**
	 * 根据已注册的 load balance 策略选择 TServiceClient，使用 key 进行 hash。
	 * 本方法仅适用于 HASH 负载均衡。
	 * @param ifaceClass Thrift Client
	 * @param key hash key
	 * @return TServiceClient
	 */
	public <X extends TServiceClient> X iface(Class<X> ifaceClass, String key) {
		return clientSelector.iface(ifaceClass, key);
    }
	
	/**
	 * 获取当前可用的所有Thrift server 列表
	 * @return 当前可用的所有Thrift server 列表
	 */
	public List<ThriftServer> getAvailableServers() {
		return clientSelector.getAvaliableServers();
	}
	
	/**
	 * 关闭连接
	 */
	public void close() {
		clientSelector.close();
    }
}
