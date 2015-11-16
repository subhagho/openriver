package com.wookler.server.common.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by subho on 16/11/15.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface CParam {
	/**
	 * TODO: <comment>
	 * 
	 * @return
	 */
	String name();

	/**
	 * TODO: <comment>
	 * 
	 * @return
	 */
	boolean required() default true;
}
