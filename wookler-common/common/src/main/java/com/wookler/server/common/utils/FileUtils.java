/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.common.utils;

import com.wookler.server.common.ConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Utility class to manage files/directories.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 11/08/14
 */
public class FileUtils {
	public static final class Constants {
		public static final String	_PARAM_TEMPDIR_	= "river.core.TEMPDIR";
		public static final String	_PARAM_WORKDIR_	= "river.core.WORKDIR";

		public static String	TEMPDIR	= System.getProperty("java.io.tmpdir");
		public static String	WORKDIR	= System.getProperty("java.io.tmpdir");
	}

	public static enum FileMode {
		Read, Wrtie, ReadWrite, Append;
	}

	/**
	 * Configure the file utility.
	 *
	 * @param params
	 *            - Configuration parameters.
	 * @throws ConfigurationException
	 */
	public static void configure(HashMap<String, String> params)
			throws ConfigurationException {
		if (params.containsKey(Constants._PARAM_TEMPDIR_)) {
			Constants.TEMPDIR = params.get(Constants._PARAM_TEMPDIR_);
			File fi = new File(Constants.TEMPDIR);
			if (!fi.exists()) {
				fi.mkdirs();
			}
		}
		if (params.containsKey(Constants._PARAM_WORKDIR_)) {
			Constants.WORKDIR = params.get(Constants._PARAM_WORKDIR_);
			File fi = new File(Constants.WORKDIR);
			if (!fi.exists()) {
				fi.mkdirs();
			}
		}
	}

	public static String createWorkFile(String ext, boolean append)
			throws IOException {
		String filename = UUID.randomUUID().toString();
		return createWorkFile(filename, ext, append);
	}

	public static String createWorkFile(String filename, String ext,
			boolean append) throws IOException {
		return createFile(filename, ext, Constants.WORKDIR, append);
	}

	public static String createTempFile(String ext) throws IOException {
		String filename = UUID.randomUUID().toString();
		return createTempFile(filename, ext, false);
	}

	public static String createTempFile(String filename, String ext,
			boolean append) throws IOException {
		return createFile(filename, ext, Constants.TEMPDIR, append);
	}

	private static String createFile(String filename, String ext, String parent,
			boolean append) throws IOException {
		if (filename == null) {
			filename = UUID.randomUUID().toString();
		}
		if (ext != null && !ext.isEmpty()) {
			if (!ext.startsWith(".")) {
				ext = "." + ext;
			}
			filename = filename + ext;
		}
		String file = parent + "/" + filename;
		File fi = new File(file);
		if (fi.exists() && !append) {
			return null;
		}
		return fi.getAbsolutePath();
	}

	/**
	 * Create a randomly named folder in the temp space.
	 *
	 * @return - Absolute path of the new directory.
	 * @throws IOException
	 */
	public static String createTempFolder() throws IOException {
		String path = UUID.randomUUID().toString();
		return createTempFolder(path, false);
	}

	/**
	 * Create a sub-folder under the temp space.
	 *
	 * @param path
	 *            - Path to create. Will be created recursively if required.
	 * @param empty
	 *            - Empty the contents if the target path already exists?
	 * @return - Absolute path of the new directory.
	 * @throws IOException
	 */
	public static String createTempFolder(String path, boolean empty)
			throws IOException {
		return createFolder(path, Constants.TEMPDIR, empty);
	}

	/**
	 * Create a randomly named folder in the workspace.
	 *
	 * @return - Absolute path of the new directory.
	 * @throws IOException
	 */
	public static String createWorkFolder() throws IOException {
		String path = UUID.randomUUID().toString();
		return createWorkFolder(path, false);
	}

	/**
	 * Create a sub-folder under the workspace.
	 *
	 * @param path
	 *            - Path to create. Will be created recursively if required.
	 * @param empty
	 *            - Empty the contents if the target path already exists?
	 * @return - Absolute path of the new directory.
	 * @throws IOException
	 */
	public static String createWorkFolder(String path, boolean empty)
			throws IOException {
		return createFolder(path, Constants.WORKDIR, empty);
	}

	public static String createFolder(String path, String parent, boolean empty)
			throws IOException {
		String dir = path + "/" + parent;
		File di = new File(dir);
		if (!di.exists()) {
			di.mkdirs();
		}
		if (empty)
			emptydir(di.getAbsolutePath());
		return di.getAbsolutePath();
	}

	/**
	 * Empty the specified directory of its content.
	 *
	 * @param path
	 *            - Directory path.
	 * @throws IOException
	 */
	public static void emptydir(String path) throws IOException {
		File di = new File(path);
		if (di.exists() && di.isDirectory()) {
			emptydir(di, false);
		}
	}

	/**
	 * Empty the specified directory of its content. If self is true, then
	 * delete the current directory.
	 *
	 * @param path
	 *            - Directory path.
	 * @param self
	 *            - Delete the containing directory?
	 * @throws IOException
	 */
	public static void emptydir(File path, boolean self) throws IOException {
		File[] files = path.listFiles();
		if (files != null && files.length > 0) {
			for (File fi : files) {
				if (fi.isDirectory()) {
					emptydir(fi, true);
				}
				fi.delete();
			}
		}
		if (self)
			path.delete();
	}

	/**
	 * Check if directory with path exists, if not create.
	 *
	 * @param path
	 * @throws Exception
	 */
	public static void checkAndCreateFolder(String path) throws IOException {
		File di = new File(path);
		if (!di.exists()) {
			di.mkdirs();
		} else {
			if (!di.isDirectory())
				throw new IOException(
						"Invalid path, file exists but is not a directory.");
		}

	}

	/**
	 * Get all the directory paths under the specified folder. It will only
	 * return the complete paths not the
	 * sub-paths.
	 *
	 * @param folder
	 *            - Folder to search under.
	 * @return - List of directory paths.
	 */
	public static List<File> paths(File folder) {
		List<File> files = null;
		File[] fs = folder.listFiles();
		if (fs != null && fs.length > 0) {
			for (File f : fs) {
				if (f.isDirectory()) {
					if (files == null)
						files = new ArrayList<File>();
					List<File> cfs = paths(f);
					if (cfs == null || cfs.isEmpty())
						files.add(f);
					else {
						files.addAll(cfs);
					}
				}
			}
		}
		return files;
	}
}
