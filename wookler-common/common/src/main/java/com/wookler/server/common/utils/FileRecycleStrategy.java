/*
 * Copyright 2014 Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * TODO: <comments>
 * @file FileRecycleStrategy.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 18-Nov-2015
 */
@CPath(path = "fileRecycle")
public abstract class FileRecycleStrategy implements Configurable {
	@CParam(name = "@backup", required = false)
	protected boolean backup = false;

	@CParam(name = "backup.directory", required = false)
	protected File backupDirectory;

	@CParam(name = "backup.count", required = false)
	protected int backupCount = 1;

	private AtomicInteger sequence = new AtomicInteger(0);

	/**
	 * @return the backup
	 */
	public boolean isBackup() {
		return backup;
	}

	/**
	 * @param backup
	 *            the backup to set
	 */
	public void setBackup(boolean backup) {
		this.backup = backup;
	}

	/**
	 * @return the backupDirectory
	 */
	public File getBackupDirectory() {
		return backupDirectory;
	}

	/**
	 * @param backupDirectory
	 *            the backupDirectory to set
	 */
	public void setBackupDirectory(File backupDirectory) {
		this.backupDirectory = backupDirectory;
	}

	/**
	 * @return the backupCount
	 */
	public int getBackupCount() {
		return backupCount;
	}

	/**
	 * @param backupCount
	 *            the backupCount to set
	 */
	public void setBackupCount(int backupCount) {
		this.backupCount = backupCount;
	}

	/**
	 * 
	 * Check if the specified file needs to be recycled.
	 * 
	 * @param file
	 *            - File to be checked.
	 * @return - Needs Recycle?
	 */
	public abstract boolean needsRecycle(File file);

	public File backup(File source) throws Exception {
		if (backup) {
			if (!backupDirectory.exists()) {
				backupDirectory.mkdirs();
			}
			File[] fList = backupDirectory.listFiles();
			if (fList != null && fList.length >= backupCount) {
				Path todelete = null;
				BasicFileAttributes tdattr = null;
				for (File f : fList) {
					if (todelete == null) {
						todelete = Paths.get(f.getAbsolutePath());
						tdattr = Files.readAttributes(todelete,
								BasicFileAttributes.class);
						continue;
					} else {
						Path p = Paths.get(f.getAbsolutePath());
						BasicFileAttributes attr = Files.readAttributes(p,
								BasicFileAttributes.class);
						if (attr.creationTime().toMillis() < tdattr
								.creationTime().toMillis()) {
							todelete = p;
							tdattr = attr;
						}
					}
				}
			}
		}
		return null;
	}
}
