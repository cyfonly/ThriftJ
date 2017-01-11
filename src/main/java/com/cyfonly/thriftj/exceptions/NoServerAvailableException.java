package com.cyfonly.thriftj.exceptions;


/**
 * 自定义 Exception
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class NoServerAvailableException extends RuntimeException{
	private static final long serialVersionUID = 1L;
	
	public NoServerAvailableException() {
        super();
    }
	
	public NoServerAvailableException(String s) {
        super(s);
    }
	
	public NoServerAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoServerAvailableException(Throwable cause) {
        super(cause);
    }
}
