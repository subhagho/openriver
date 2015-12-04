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

package com.wookler.server.river;

import com.wookler.server.common.*;
import com.wookler.server.common.config.CParam;
import com.wookler.server.common.config.CPath;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Backup and Recovery handler for message blocks.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 16/08/14
 */
@CPath(path = "backup")
public class MessageBlockBackup implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(MessageBlockBackup.class);

    public static final class Constants {
        public static final String DIR_DATETIME_FORMAT = "/yyyy/MM/dd/HH";
    }

    /** Message Block backup retention period */
    @CParam(name = "backup.retention")
    private String retentionValue;
    /** time window corresponding to retention period */
    private TimeWindow retention;
    /** backup directory */
    @CParam(name = "backup.directory")
    private File destination;
    /** MessageBlockBackup setState */
    private ObjectState state = new ObjectState();
    /** Message queue name */
    private String qname;

    /**
     * Configure the backup handler.
     *
     * @param config
     *            - Configuration node for this instance.
     * @throws com.wookler.server.common.ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            LogUtils.debug(getClass(), ((ConfigPath) config).path());

            ConfigUtils.parse(config, this);

            LogUtils.debug(getClass(), "[ Backup Directory:" + destination.getAbsolutePath() + "]");
            if (!destination.exists())
                destination.mkdirs();

            LogUtils.debug(getClass(), "[ Backup Retention: " + retentionValue + "]");
            retention = TimeWindow.parse(retentionValue);

            state.setState(EObjectState.Available);
        } catch (ConfigurationException e) {
            state.setState(EObjectState.Exception).setError(e);
            throw e;
        } catch (TimeWindowException e) {
            state.setState(EObjectState.Exception).setError(e);
            state.setError(e);
            throw new ConfigurationException("Error reading retention time window.", e);
        }
    }

    /**
     * Get the configured destination folder for backups.
     *
     * @return - Backup destination directory.
     */
    public File destination() {
        return destination;
    }

    /**
     * Recover a message block from the specified source folder. The recovered
     * files are stored at the specified destination directory.
     *
     * @param srcdir
     *            - Source directory where the message block was backed up.
     * @param destdir
     *            - Destination directory to store the restored files.
     * @return - Recovered message block.
     * @throws BlockBackupException
     */
    public MessageBlock recover(String srcdir, String destdir) throws BlockBackupException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            File di = new File(srcdir);
            if (!di.exists() || !di.isDirectory())
                throw new BlockBackupException(
                        "Invalid backup source directory specified. [directory="
                                + di.getAbsolutePath() + "]");
            File destd = new File(destdir);
            if (!destd.exists())
                destd.mkdirs();

            // Append the time based directory based on the input folders time
            // window.
            String tdir = windowdir(di);
            File df = new File(destd.getAbsoluteFile() + tdir);
            if (!df.exists())
                df.mkdirs();

            File[] files = di.listFiles();
            if (files == null || files.length <= 0)
                throw new BlockBackupException(
                        "Invalid backup source directory specified. No files to recover. [directory="
                                + di.getAbsolutePath() + "]");
            for (File f : files) {
                recoverfile(f, df);
            }

            return new MessageBlock(destd.getName(), destd.getParentFile().getAbsolutePath(),
                    qname, true, null).recovered(true);
        } catch (StateException e) {
            throw new BlockBackupException("Error backing up block. [directory=" + srcdir + "]", e);
        } catch (MessageQueueException e) {
            throw new BlockBackupException("Error backing up block. [directory=" + srcdir + "]", e);
        }
    }

    /**
     * Backup the specified message block.
     *
     * @param block
     *            - Message block to backup
     * @throws BlockBackupException
     */
    public void backup(MessageBlock block) throws BlockBackupException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            File bf = new File(block.directory());
            if (!bf.exists() || !bf.isDirectory())
                throw new BlockBackupException("Invalid block directory. [directory="
                        + block.directory() + "]");
            log.info(String.format("Backing up Message Block : ID=%s, directory=%s", block.id(),
                    block.directory()));

            String dname = bf.getName();
            String ddname = windowdir(block.createtime()) + "/" + dname;
            File dd = new File(ddname);
            if (!dd.exists())
                dd.mkdirs();

            log.info(String.format("Backing files to [%s].", dd.getAbsolutePath()));

            File[] files = bf.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    backupfile(f, dd);
                }
            }
        } catch (StateException e) {
            throw new BlockBackupException("Error backing up block. [block=" + block.directory()
                    + "]", e);
        }
    }

    /**
     * Clean up older backup files based on the configured retention period.
     *
     * @throws BlockBackupException
     */
    public void cleanup() throws BlockBackupException {
        try {
            ObjectState.check(state, EObjectState.Available, getClass());
            long[] w = retention.window(System.currentTimeMillis());
            List<File> directories = FileUtils.paths(destination);
            if (directories != null && !directories.isEmpty()) {
                for (File d : directories) {
                    checkAndClean(d, w);
                }
            }
        } catch (StateException e) {
            throw new BlockBackupException("Error cleaning up.", e);
        } catch (TimeWindowException e) {
            throw new BlockBackupException("Error cleaning up.", e);
        }
    }

    /**
     * Get the setState of this backup handler.
     *
     * @return - Instance setState.
     */
    public ObjectState state() {
        return state;
    }

    /**
     * Dispose this backup handler.
     */
    @Override
    public void dispose() {
        if (state.getState() != EObjectState.Exception)
            state.setState(EObjectState.Disposed);
    }

    /**
     * Check and clean the backup file based on time window
     *
     * @param dir
     *            the dir to be checked and cleaned
     * @param window
     *            the time window against which the check should be made
     * @throws BlockBackupException
     *             the block backup exception
     */
    private void checkAndClean(File dir, long[] window) throws BlockBackupException {
        try {
            long fts = windowtime(dir);
            if (fts > 0) {
                if (fts < window[0]) {
                    FileUtils.emptydir(dir, true);
                }
            }
        } catch (IOException e) {
            throw new BlockBackupException("Error removing directory. [directory="
                    + dir.getAbsolutePath() + "]", e);
        }
    }

    /**
     * Recover the file to specified destination. Files are uncompressed while
     * recovering.
     *
     * @param f
     *            the file to be recovered
     * @param dest
     *            the dest dir
     * @throws BlockBackupException
     *             the block backup exception
     */
    private void recoverfile(File f, File dest) throws BlockBackupException {
        log.info(String.format("Uncompressing file [%s]...", f.getAbsolutePath()));
        try {
            String fname = dest.getAbsolutePath() + "/" + f.getName();
            File fi = new File(fname);
            if (fi.exists())
                fi.delete();

            SnappyFramedInputStream is = new SnappyFramedInputStream(new FileInputStream(f));
            FileOutputStream os = new FileOutputStream(fi);

            try {
                byte[] buff = new byte[4096];
                while (true) {
                    int r = is.read(buff);
                    if (r < 0)
                        break;
                    os.write(buff, 0, r);
                }
            } finally {
                is.close();
                os.close();
            }
        } catch (IOException e) {
            throw new BlockBackupException("Error un-compressing file. [file="
                    + f.getAbsolutePath() + "]", e);
        }
    }

    /**
     * Backup file to the specified destination. Files are compressed before
     * backup
     *
     * @param f
     *            the file to be backed up
     * @param dest
     *            the dest dir where the backup file is placed
     * @throws BlockBackupException
     *             the block backup exception
     */
    private void backupfile(File f, File dest) throws BlockBackupException {
        try {
            String fname = dest.getAbsolutePath() + "/" + f.getName();

            File fi = new File(fname);
            if (fi.exists())
                fi.delete();
            log.info(String.format("Backing up source [%s] to [%s]", f.getAbsolutePath(),
                    fi.getAbsolutePath()));
            SnappyFramedOutputStream os = new SnappyFramedOutputStream(new FileOutputStream(fi));
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
            throw new BlockBackupException("Error backing up file. [file=" + f.getAbsolutePath()
                    + "]", e);
        }

    }

    private long windowtime(File dir) throws BlockBackupException {
        String dts = windowdir(dir);

        if (!StringUtils.isEmpty(dts)) {
            try {
                DateTimeFormatter fmt = DateTimeFormat.forPattern(Constants.DIR_DATETIME_FORMAT);
                DateTime t = fmt.parseDateTime(dts);

                return t.getMillis();
            } catch (IllegalArgumentException e) {
                return -1;
            }
        }
        return -1;
    }

    private String windowdir(File dir) throws BlockBackupException {
        StringBuffer b = new StringBuffer();

        // Get the parent (Hour) dir.
        File p = dir.getParentFile();
        if (p == null || !p.isDirectory())
            throw new BlockBackupException("Invalid directory : Error getting hour. [directory="
                    + p.getAbsolutePath() + "]");
        b.append("/").append(p.getName());

        // Day dir
        p = p.getParentFile();
        if (p == null || !p.isDirectory())
            throw new BlockBackupException("Invalid directory : Error getting day. [directory="
                    + p.getAbsolutePath() + "]");
        b.insert(0, p.getName()).insert(0, "/");

        // Month dir
        p = p.getParentFile();
        if (p == null || !p.isDirectory())
            throw new BlockBackupException("Invalid directory : Error getting month. [directory="
                    + p.getAbsolutePath() + "]");
        b.insert(0, p.getName()).insert(0, "/");

        // Year dir
        p = p.getParentFile();
        if (p == null || !p.isDirectory())
            throw new BlockBackupException("Invalid directory : Error getting year. [directory="
                    + p.getAbsolutePath() + "]");
        b.insert(0, p.getName()).insert(0, "/");

        return b.toString();
    }

    private String windowdir(long timestamp) {
        StringBuffer b = new StringBuffer(destination.getAbsolutePath());
        DateTime t = new DateTime(timestamp);
        b.append(t.toString(Constants.DIR_DATETIME_FORMAT));
        return b.toString();
    }

    /**
     * Exception type to be used for Backup related problems.
     */
    @SuppressWarnings("serial")
    public static class BlockBackupException extends Exception {
        private static final String _PREFIX_ = "Message Block Backup Error : ";

        /**
         * Instantiates a new block backup exception with exception message
         *
         * @param mesg
         *            the exception mesg
         */
        public BlockBackupException(String mesg) {
            super(_PREFIX_ + mesg);
        }

        /**
         * Instantiates a new block backup exception with exception message and
         * exception cause
         *
         * @param mesg
         *            the exception mesg
         * @param inner
         *            the exception cause
         */
        public BlockBackupException(String mesg, Throwable inner) {
            super(_PREFIX_ + mesg, inner);
        }
    }

    /**
     * Get the backup directory
     * 
     * @return the destination
     */
    public File getDestination() {
        return destination;
    }

    /**
     * Set the backup directory
     * 
     * @param destination
     *            the destination to set
     */
    public void setDestination(File destination) {
        this.destination = destination;
    }

    /**
     * Get the queue name
     * 
     * @return the qname
     */
    public String getQname() {
        return qname;
    }

    /**
     * Set the queue name
     * 
     * @param qname
     *            the qname to set
     */
    public void setQname(String qname) {
        this.qname = qname;
    }

    /**
     * Get the MessageBlockBackup setState
     * 
     * @return the state
     */
    public ObjectState getState() {
        return state;
    }

    /**
     * Get the backup retention
     * 
     * @return the retentionValue
     */
    public String getRetentionValue() {
        return retentionValue;
    }

    /**
     * Set the backup retention
     * 
     * @param retentionValue
     *            the retentionValue to set
     */
    public void setRetentionValue(String retentionValue) {
        this.retentionValue = retentionValue;
    }
}
