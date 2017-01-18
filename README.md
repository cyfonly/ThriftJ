![design](https://github.com/cyfonly/ThriftJ/blob/master/pictures/ThriftJ.png "ThriftJ")  
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/cyfonly/ThriftJ/blob/master/LICENSE)  [![Built with Maven](http://maven.apache.org/images/logos/maven-feather.png)](http://search.maven.org/#search%7Cga%7C1%7Ccyfonly)  
A failover and load balance Thrift client with pooled connections  
# Features  
1. 链式调用API，简洁直观
2. 完善的默认配置，无需担心调用时配置不全导致抛错
3. 池化连接对象，高效管理连接的生命周期
4. 异常服务自动隔离与恢复
5. 多种可配置的负载均衡策略，支持随机、轮询、权重和哈希
6. 多种可配置的服务级别，并自动根据服务级别进行服务降级  
  
# Architectural Design  
![design](https://github.com/cyfonly/ThriftJ/blob/master/pictures/ThriftJ_design.png "ThriftJ_design.png")  

# Usage  
#### 1. Maven
```
<dependency>
    <groupId>com.github.cyfonly</groupId>
    <artifactId>thriftj</artifactId>
    <version>1.0.1</version>
</dependency>
<dependency>
    log4j or logback, and so on...
</dependency>
```  
#### 2. 调用  
>除 servers 必须配置外，其他配置均为可选（使用默认配置）  

```
//Thrift server 列表
private static final String servers = "127.0.0.1:10001,127.0.0.1:10002";

//TTransport 验证器
ConnectionValidator validator = new ConnectionValidator() {
    @Override
    public boolean isValid(TTransport object) {
        return object.isOpen();
    }
};

//连接对象池配置
GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();

//failover 策略
FailoverStrategy failoverStrategy = new FailoverStrategy();

//构造 ThriftClient 对象并配置
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
            
//打印从 ThriftClient 获取到的可用服务列表
List<ThriftServer> servers = thriftClient.getAvailableServers();
for(ThriftServer server : servers){
    System.out.println(server.getHost() + ":" + server.getPort());
}

//服务调用
if(servers.size()>0){
    try{
		    TestThriftJ.Client client = thriftClient.iface(TestThriftJ.Client.class);
		    QryResult result = client.qryTest(1);
		    System.out.println("result[code=" + result.code + " msg=" + result.msg + "]");
	  }catch(Throwable t){
		    logger.error("-------------exception happen", t);
	  }
}
```  


# Demo  
see [https://github.com/cyfonly/ThriftJ/tree/master/src/test](https://github.com/cyfonly/ThriftJ/tree/master/src/test)  

# License  
基于 Apache License 2.0 发布。有关详细信息，请参阅 [LICENSE](https://github.com/cyfonly/ThriftJ/blob/master/LICENSE)。
