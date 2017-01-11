package com.cyfonly.thriftj.test.thriftserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cyfonly.thriftj.test.thriftserver.thrift.TestThriftJ;


/**
 * TestThriftJ.thrift server
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ThriftServerTest {
	private static final Logger logger = LoggerFactory.getLogger(ThriftServerTest.class);
	
	private final static int[] ports = {10001};
	
	public static void main(String[] args){
		ExecutorService es = Executors.newFixedThreadPool(2);
		for(int i=0; i<ports.length; i++){
			final int index = i;
			es.execute(new Runnable() {
				@Override
				public void run() {
					try{
						TNonblockingServerSocket socket = new TNonblockingServerSocket(ports[index]);
						TestThriftJ.Processor processor = new TestThriftJ.Processor(new QueryImp());
						TNonblockingServer.Args arg = new TNonblockingServer.Args(socket);
						arg.protocolFactory(new TBinaryProtocol.Factory());
						arg.transportFactory(new TFramedTransport.Factory());
						arg.processorFactory(new TProcessorFactory(processor));
						TServer server = new TNonblockingServer(arg);
						
						logger.info("127.0.0.1:" + ports[index] + " start");
						server.serve();
					}catch(Exception e){
						logger.error("127.0.0.1:" + ports[index] + " error");
					}
				}
			});
		}
	}
	
}
