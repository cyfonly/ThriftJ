package com.cyfonly.thriftj.test.thriftserver;
import org.apache.thrift.TException;

import com.cyfonly.thriftj.test.thriftserver.thrift.QryResult;
import com.cyfonly.thriftj.test.thriftserver.thrift.TestThriftJ;


/**
 * TestThriftJ.thrift 查询接口实现
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class QueryImp implements TestThriftJ.Iface{

	@Override
	public QryResult qryTest(int qryCode) throws TException {
		QryResult result = new QryResult();
		if(qryCode==0){
			result.code = 0;
			result.msg = "success";
		}else{
			result.code = 1;
			result.msg = "fail";
		}
		return result;
	}

}
