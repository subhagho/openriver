/**
 * TODO: <comments>
 *
 * @file FileBackupHelper.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedOutputStream;

import com.google.common.base.Preconditions;
import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigUtils;

/**
 * TODO: <comment>
 *
 * @author subho
 * @date 18-Nov-2015
 */
@CPath(path = "backup")
public class FileBackupHelper implements Configurable {
	private static final Logger log = LoggerFactory
			.getLogger(FileBackupHelper.class);

	public static final class Constants {
		public static final String	FILE_EXT_ZIP	= ".zip";
		public static final String	FILE_EXT_BACKUP	= ".bak";

		public static final String getZipFilename(String name) {
			return String.format("%s%s", name, FILE_EXT_ZIP);
		}

		public static final String getBackupFilename(String name) {
			return String.format("%s%s", name, FILE_EXT_BACKUP);
		}

		public static boolean isBackupFile(String name) {
			return (name.endsWith(FILE_EXT_BACKUP)
					|| name.endsWith(FILE_EXT_ZIP));
		}
	}

	@CParam(name = "backup.directory", required = false)
	private File backupDirectory;

	@CParam(name = "backup.count", required = false)
	private int backupCount = 1;

	@CParam(name = "backup.compress", required = false)
	private boolean compress = false;

	private AtomicInteger sequence = new AtomicInteger(0);

	/**
	 * Check if compression is enabled.
	 * 
	 * @return the compress
	 */
	public boolean isCompress() {
		return compress;
	}

	/**
	 * Turn compression On/Off.
	 * 
	 * @param compress
	 *            the compress to set
	 */
	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	/**
	 * Get the backup directory handle.
	 * 
	 * @return the backupDirectory
	 */
	public File getBackupDirectory() {
		return backupDirectory;
	}

	/**
	 * Set the backup directory.
	 * 
	 * @param backupDirectory
	 *            the backupDirectory to set
	 */
	public void setBackupDirectory(File backupDirectory) {
		this.backupDirectory = backupDirectory;
	}

	/**
	 * Get the Max number of backup files to keep. If backupCount is set to less
	 * than 0 all files will be backed up.
	 * 
	 * @return the backupCount
	 */
	public int getBackupCount() {
		return backupCount;
	}

	/**
	 * Set the Max number of backup files to keep. If backupCount is set to less
	 * than 0 all files will be backed up.
	 * 
	 * @param backupCount
	 *            the backupCount to set
	 */
	public void setBackupCount(int backupCount) {
		this.backupCount = backupCount;
	}

	/**
	 * Backup the specified file to the configured backup directory. If
	 * compression is enabled will compress using Snappy compression.
	 * 
	 * @param source
	 *            - File handle to backup.
	 * @return - File handle for the backup file created.
	 * @throws Exception
	 */
	public File backup(File source) throws Exception {
		Preconditions.checkArgument(source != null && source.exists());

		LogUtils.debug(getClass(),
				"Backup invoked : file=" + source.getAbsolutePath() + "]", log);
		checkBackupSize();
		String fname = getBackupFilename(source);
		File backf = new File(fname);
		if (backf.exists())
			backf.delete();
		if (compress) {
			compress(source, backf);
			source.delete();
		} else {
			source.renameTo(backf);
		}
		return backf;
	}

	private void compress(File f, File dest) throws Exception {
		try {
			log.info(String.format("Backing up source [%s] to [%s]",
					f.getAbsolutePath(), dest.getAbsolutePath()));
			SnappyFramedOutputStream os = new SnappyFramedOutputStream(
					new FileOutputStream(dest));
			FileInputStream is = new FileInputStream(f);
			try {
				byte[] buff = new byte[4096];
				while (true) {
					int r = is.read(buff);
					if (r < 0)
						break;
					os.write(buff, 0, r);
				}
			} finally {
				os.close();
				is.close();
			}
		} catch (IOException e) {
			throw new Exception(
					"Error backing up file. [file=" + f.getAbsolutePath() + "]",
					e);
		}

	}

	private String getBackupFilename(File source) {
		String cf = String.format("%s:%d", TimeUtils.now("yyyy.MM.dd.HH.mm"),
				sequence.incrementAndGet());
		String fname = FileUtils.insertExtension(source, cf);
		if (compress)
			fname = Constants.getZipFilename(fname);
		else
			fname = Constants.getBackupFilename(fname);

		fname = String.format("%s/%s", backupDirectory.getAbsolutePath(),
				fname);
		return fname;
	}

	private void checkBackupSize() throws Exception {
		if (backupCount < 0)
			return;

		FilenameFilter filter = new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				name = name.toLowerCase();
				return Constants.isBackupFile(name);
			}
		};

		File[] fList = backupDirectory.listFiles(filter);
		if (fList != null && fList.length >= backupCount) {
			int deleteCount = fList.length - backupCount + 1;
			Map<Path, BasicFileAttributes> deleteMap = new HashMap<>();

			for (File f : fList) {
				if (!f.isFile())
					continue;

				Path p = Paths.get(f.getAbsolutePath());
				BasicFileAttributes attrs = Files.readAttributes(p,
						BasicFileAttributes.class);
				if (deleteMap.size() < deleteCount) {
					deleteMap.put(p, attrs);
				} else {
					boolean delete = false;
					Map<Path, BasicFileAttributes> oMap = new HashMap<>();
					for (Path pd : deleteMap.keySet()) {
						BasicFileAttributes dattrs = deleteMap.get(pd);
						if (isOlderFile(attrs, dattrs)) {
							if (!delete)
								delete = true;
							oMap.put(pd, dattrs);
						}
					}
					if (delete) {
						if (!oMap.isEmpty()) {
							Path pd = findNewest(oMap);
							if (pd != null) {
								deleteMap.remove(pd);
							}
						}
						deleteMap.put(p, attrs);
					}
				}
			}
			if (!deleteMap.isEmpty()) {
				for (Path p : deleteMap.keySet()) {
					p.toFile().delete();
				}
			}
		}
	}

	private Path findNewest(Map<Path, BasicFileAttributes> oMap) {
		Path pn = null;
		BasicFileAttributes an = null;
		for (Path p : oMap.keySet()) {
			BasicFileAttributes ba = oMap.get(p);
			if (pn == null) {
				pn = p;
				an = ba;
			} else {
				if (!isOlderFile(ba, an)) {
					pn = p;
					an = ba;
				}
			}
		}
		return pn;
	}

	private boolean isOlderFile(BasicFileAttributes source,
			BasicFileAttributes target) {
		return source.creationTime().compareTo(target.creationTime()) < 0;
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#configure(com.wookler.server.
	 * common.config.ConfigNode)
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		try {
			ConfigUtils.parse(config, this);
			if (!backupDirectory.exists()) {
				backupDirectory.mkdirs();
			} else {
				checkBackupSize();
			}
		} catch (Exception e) {
			throw new ConfigurationException(
					"Error configuring File Backup helper.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		// TODO Auto-generated method stub

	}

}
