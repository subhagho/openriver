/**
 * TODO: <comments>
 *
 * @file FileSizeRecycleStrategy.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 18-Nov-2015
 */
@CPath(path = "recycleStrategy")
public class FileSizeRecycleStrategy extends FileRecycleStrategy {
	@CParam(name = "size")
	private long maxSize;

	/**
	 * @return the maxSize
	 */
	public long getMaxSize() {
		return maxSize;
	}

	/**
	 * @param maxSize
	 *            the maxSize to set
	 */
	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#configure(com.wookler.server.
	 * common.config.ConfigNode)
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		setup(config);
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		// Do nothing...
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * com.wookler.server.common.utils.FileRecycleStrategy#needsRecycle(java.io.
	 * File)
	 */
	@Override
	public boolean needsRecycle(File file) {
		try {
			Path p = Paths.get(file.getAbsolutePath());
			BasicFileAttributes attrs = Files.readAttributes(p,
					BasicFileAttributes.class);
			if (attrs.size() > maxSize)
				return true;
		} catch (Exception e) {
			LogUtils.warn(getClass(), e.getLocalizedMessage());
			LogUtils.stacktrace(getClass(), e);
		}
		return false;
	}

}
