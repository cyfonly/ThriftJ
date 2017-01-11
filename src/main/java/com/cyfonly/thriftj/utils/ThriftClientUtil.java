package com.cyfonly.thriftj.utils;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Sets;


/**
 * 
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class ThriftClientUtil {
	
	private static ConcurrentMap<Class<?>, Set<String>> interfaceMethodCache = new ConcurrentHashMap<>();
	
	public static final int randomNextInt() {
		return ThreadLocalRandom.current().nextInt();
	}
	
	public static final Set<String> getInterfaceMethodNames(Class<?> ifaceClass) {
		if (interfaceMethodCache.containsKey(ifaceClass))
			return interfaceMethodCache.get(ifaceClass);
		Set<String> methodName = Sets.newHashSet();
		Class<?>[] interfaces = ifaceClass.getInterfaces();
		for (Class<?> class1 : interfaces) {
			Method[] methods = class1.getMethods();
			for (Method method : methods) {
				methodName.add(method.getName());
			}
		}
		interfaceMethodCache.putIfAbsent(ifaceClass, methodName);
		return methodName;
	}
	
	public static final int chooseWithWeight(int[] weights) {
		int count = weights.length;
		int sum = 0;
		
		for (int i = 0; i < count; i++) {
			sum += weights[i];
		}

		int random = ThreadLocalRandom.current().nextInt(sum);

		while (random + weights[count - 1] < sum) {
			sum -= weights[--count];
		}
		return count - 1;
	}
	
}
