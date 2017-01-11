package com.cyfonly.thriftj.pool;

import org.apache.thrift.transport.TTransport;


/**
 * Thrift 连接池抽象接口
 * @author yunfeng.cheng
 * @create 2016-11-18
 */
public interface ThriftConnectionPool {
	
	TTransport getConnection(ThriftServer thriftServer);

    void returnConnection(ThriftServer thriftServer, TTransport transport);

    void returnBrokenConnection(ThriftServer thriftServer, TTransport transport);

	void close();

    void clear(ThriftServer thriftServer);

}
