/**
* 测试ThriftJ(基于thrift-0.6.1)
* yunfeng.cheng
* 2016-11-21
**/
namespace java com.cyfonly.thriftj.test.thriftserver.thrift

struct QryResult {
	1:i32 code; 
	2:string msg;
}
service TestThriftJ{
	/**
	 * 简单的测试查询接口，当qryCode=0时返回QryResult[code=0,msg=success]，qryCode值为其他值时返回QryResult[code=1,msg=fail]
	 */
	QryResult qryTest(1:i32 qryCode)
}