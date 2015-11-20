/**
 * TODO: <comments>
 *
 * @file RecycledFileOutput.java
 * @author subho
 * @date 18-Nov-2015
 */
package com.wookler.server.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.Env;
import com.wookler.server.common.ManagedTask;
import com.wookler.server.common.Task.TaskException;
import com.wookler.server.common.TaskState;
import com.wookler.server.common.TaskState.ETaskState;
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
@CPath(path = "recycled-file")
public class RecycledFileOutput implements Configurable {
	private static final Logger log = LoggerFactory
			.getLogger(RecycledFileOutput.class);

	@CParam(name = "recycleStrategy", required = false, nested = true)
	private FileRecycleStrategy	recycleStrategy;
	private File				directory;
	@CParam(name = "@filename", required = false)
	private File				file;
	private FileOutputStream	stream;
	@CParam(name = "@append", required = false)
	private boolean				append		= false;
	@CParam(name = "backup", required = false, nested = true)
	private FileBackupHelper	backupHelper;
	private ReentrantLock		recycleLock	= new ReentrantLock();
	private AtomicInteger		sequence	= new AtomicInteger(0);
	private Queue<File>			backupQueue	= null;
	private Runner				runner		= null;

	public RecycledFileOutput() {
	}

	/**
	 * TODO: <comment>
	 * 
	 * RecycledFileOutput
	 *
	 */
	public RecycledFileOutput(String name) {
		setup(name, null, false);
	}

	public RecycledFileOutput(String name, boolean append) {
		setup(name, null, append);
	}

	public RecycledFileOutput(String name, FileRecycleStrategy strategy) {
		setup(name, strategy, false);
	}

	public RecycledFileOutput(String name, FileRecycleStrategy strategy,
			boolean append) {
		setup(name, strategy, append);
	}

	private void setup(String name, FileRecycleStrategy strategy,
			boolean append) {
		this.file = new File(name);
		this.directory = file.getParentFile();
		if (!this.directory.exists()) {
			this.directory.mkdirs();
		}
		this.append = append;
		this.recycleStrategy = strategy;
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#configure(com.wookler.server.
	 * common.config.ConfigNode)
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		try {
			boolean doSetup = (file == null);
			ConfigUtils.parse(config, this);
			if (doSetup) {
				this.directory = file.getParentFile();
			}
			if (backupHelper != null) {
				backupQueue = new LinkedBlockingQueue<>();
				runner = new Runner(backupQueue, backupHelper);
				Env.get().taskmanager().addtask(runner);
			}
			stream = new FileOutputStream(file, append);
		} catch (Exception e) {
			throw new ConfigurationException(
					"Error configuring Recycled file handle.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		try {
			if (stream != null) {
				stream.flush();
				stream.close();
			}
			if (recycleStrategy != null) {
				recycleStrategy.dispose();
			}
			if (backupHelper != null) {
				backupHelper.dispose();
			}
			if (runner != null) {
				runner.dispose();
			}
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			LogUtils.error(getClass(), e);
		}
	}

	/**
	 * Write the data to the file. Method will check if the current file needs
	 * to be recycled.
	 * 
	 * @param data
	 *            - Data bytes to write
	 * @throws IOException
	 */
	public void write(byte[] data) throws IOException {
		checkRecycle();
		stream.write(data);
	}

	public void wrtie(byte[] data, int off, int len) throws IOException {
		checkRecycle();
		stream.write(data, off, len);
	}

	private void checkRecycle() throws IOException {
		if (recycleStrategy.needsRecycle(file)) {
			recycleLock.lock();
			try {
				if (recycleStrategy.needsRecycle(file)) {
					stream.flush();
					stream.close();

					if (backupHelper != null) {
						String fname = FileUtils.insertExtension(file,
								String.valueOf(sequence.incrementAndGet()));
						File dir = file.getParentFile();
						if (dir != null) {
							fname = String.format("%s/%s",
									dir.getAbsolutePath(), fname);
						}
						File f = new File(fname);
						if (f.exists())
							f.delete();
						File ff = new File(file.getAbsolutePath());
						ff.renameTo(f);
						backupQueue.add(f);
					}
					file = new File(file.getAbsolutePath());
					stream = new FileOutputStream(file);
				}
			} finally {
				recycleLock.unlock();
			}
		}
	}

	/**
	 * Flush the output stream.
	 * 
	 * @throws IOException
	 */
	public void flush() throws IOException {
		stream.flush();
	}

	/**
	 * Get the file instance for this handle.
	 * 
	 * @return - File instance.
	 */
	public File file() {
		return file;
	}

	/**
	 * Get the directory for this handle.
	 * 
	 * @return - Directory where the file is being written.
	 */
	public File directory() {
		return directory;
	}

	private static final class Runner implements ManagedTask {
		private static final long runInterval = 10000; // Run every 10 secs.

		private Queue<File>			backupQueue;
		private long				lastRunTime	= System.currentTimeMillis();
		private TaskState			state		= new TaskState();
		private FileBackupHelper	backupHelper;

		public Runner(Queue<File> backupQueue, FileBackupHelper backupHelper) {
			this.backupQueue = backupQueue;
			this.backupHelper = backupHelper;

			state.state(ETaskState.Runnable);
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#name()
		 */
		@Override
		public String name() {
			// TODO Auto-generated method stub
			return null;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#state()
		 */
		@Override
		public TaskState state() {
			return state;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.wookler.server.common.ManagedTask#state(com.wookler.server.common
		 * .TaskState.ETaskState)
		 */
		@Override
		public ManagedTask state(ETaskState state) {
			this.state.state(state);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#run()
		 */
		@Override
		public TaskState run() {
			if (state.state() == ETaskState.Runnable) {
				state.state(ETaskState.Running);
				try {
					while (true) {
						File f = backupQueue.poll();
						if (f == null)
							break;
						backupHelper.backup(f);
					}
				} catch (Exception e) {
					return new TaskState().state(TaskState.ETaskState.Failed)
							.error(e);
				} finally {
					lastRunTime = System.currentTimeMillis();
				}
				return new TaskState().state(TaskState.ETaskState.Success);
			}
			return state;
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#dispose()
		 */
		@Override
		public void dispose() {
			if (state.state() != TaskState.ETaskState.Exception)
				state.state(ETaskState.Stopped);
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * com.wookler.server.common.ManagedTask#response(com.wookler.server.
		 * common.TaskState)
		 */
		@Override
		public void response(TaskState state) throws TaskException {
			if (state.state() == TaskState.ETaskState.Exception
					|| state.state() == TaskState.ETaskState.Failed) {
				LogUtils.stacktrace(getClass(), state.error(), log);
				LogUtils.warn(getClass(), state.error().getLocalizedMessage());
			}
			if (state.state() != TaskState.ETaskState.Exception) {
				this.state.state(TaskState.ETaskState.Runnable);
			}
		}

		/*
		 * (non-Javadoc)
		 * @see com.wookler.server.common.ManagedTask#canrun()
		 */
		@Override
		public boolean canrun() {
			return (state.state() == ETaskState.Runnable
					&& System.currentTimeMillis() - lastRunTime > runInterval
					&& !backupQueue.isEmpty());
		}
	}

	/**
	 * @return the recycleStrategy
	 */
	public FileRecycleStrategy getRecycleStrategy() {
		return recycleStrategy;
	}

	/**
	 * @param recycleStrategy
	 *            the recycleStrategy to set
	 */
	public void setRecycleStrategy(FileRecycleStrategy recycleStrategy) {
		this.recycleStrategy = recycleStrategy;
	}

	/**
	 * @return the directory
	 */
	public File getDirectory() {
		return directory;
	}

	/**
	 * @param directory
	 *            the directory to set
	 */
	public void setDirectory(File directory) {
		this.directory = directory;
	}

	/**
	 * @return the append
	 */
	public boolean isAppend() {
		return append;
	}

	/**
	 * @param append
	 *            the append to set
	 */
	public void setAppend(boolean append) {
		this.append = append;
	}

	/**
	 * @return the backupHelper
	 */
	public FileBackupHelper getBackupHelper() {
		return backupHelper;
	}

	/**
	 * @param backupHelper
	 *            the backupHelper to set
	 */
	public void setBackupHelper(FileBackupHelper backupHelper) {
		this.backupHelper = backupHelper;
	}

	/**
	 * @return the file
	 */
	public File getFile() {
		return file;
	}

	/**
	 * @param file
	 *            the file to set
	 */
	public void setFile(File file) {
		this.file = file;
	}
}
