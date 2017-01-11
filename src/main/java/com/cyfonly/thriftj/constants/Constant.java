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

}
