package com.cyfonly.thriftj.failover;

import org.apache.thrift.transport.TTransport;

/**
 * 连接验证
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public interface ConnectionValidator {

	boolean isValid(TTransport object);
	
}
