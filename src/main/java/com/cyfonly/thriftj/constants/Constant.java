package com.cyfonly.thriftj.constants;

/**
 * 常量
 * @author yunfeng.cheng
 * @create 2016-11-18
 */
public class Constant {
	
	//--------负载均衡
	public class LoadBalance {
		/**随机**/
		public final static int RANDOM = 1;
		/**轮询**/
		public final static int ROUND_ROBIN = 2;
		/**权重**/
		public final static int WEIGHT = 3;
		/**哈希**/
		public final static int HASH = 4;
	}
	
	//--------服务级别
	public class ServiceLevel {
		/**仅使用配置的 servers 列表中可用的服务**/
		public final static int SERVERS_ONLY = 1;
		/**当 servers 列表中的服务全部不可用时，使用 backupServers 列表中的可用服务**/
		public final static int ALL_SERVERS = 2;
		/**当 servers 和 backupServers 列表中的服务全部不可用时，返回 servers 列表中的所有服务**/
		public final static int NOT_EMPTY = 3;
	}

}
