package com.wookler.server.common.config;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.utils.FileUtils;
import com.wookler.server.common.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * Created by subghosh on 3/4/15.
 */
public class URLConfigLoader {
    public static final class Constants {
        public static final String PARAM_CONFIG_URL = "config.url";
        public static final String PARAM_CONFIG_ROOT = "config.root.path";
    }

    public static Config load(String propf) throws ConfigurationException {
        try {
            Properties props = new Properties();
            FileInputStream pf = new FileInputStream(propf);
            props.load(pf);

            if (props != null && !props.isEmpty()) {
                String url = props.getProperty(Constants.PARAM_CONFIG_URL);
                if (StringUtils.isEmpty(url))
                    throw new ConfigurationException("Invalid properties specified. Configuration URL not specified. [property=" + Constants.PARAM_CONFIG_URL + "]");

                String rootPath = props.getProperty(Constants.PARAM_CONFIG_ROOT);
                if (StringUtils.isEmpty(rootPath))
                    throw new ConfigurationException("Invalid properties specified. Configuration Root Path not specified. [property=" + Constants.PARAM_CONFIG_ROOT + "]");

                System.setProperties(props);

                LogUtils.warn(URLConfigLoader.class, "Reading configuration from URL. [url=" + url + "]");
                String c_path = FileUtils.createTempFile(".config");
                File cf = new File(c_path);
                if (cf.exists()) {
                    cf.delete();
                }

                FileOutputStream fos = new FileOutputStream(cf);
                URL c_url = new URL(url);
                BufferedReader reader = new BufferedReader(new InputStreamReader(c_url.openStream()));
                try {
                    while(true) {
                        String line = reader.readLine();
                        if (line == null)
                            break;
                        if (StringUtils.isEmpty(line))
                            continue;
                        fos.write(line.getBytes("UTF-8"));
                    }
                } finally {
                    if (reader != null)
                        reader.close();
                    if (fos != null)
                        fos.close();
                }
                LogUtils.warn(URLConfigLoader.class, "Created local copy. [path=" + cf.getAbsolutePath() + "]");
                if (!cf.exists())
                    throw new ConfigurationException("Error getting file from URL.");
                Config config = new Config(cf.getAbsolutePath(), rootPath);

                return config;
            } else {
                throw new ConfigurationException("Invalid properties specified. No properties loaded.");
            }
        } catch (Throwable e) {
            LogUtils.stacktrace(URLConfigLoader.class, e);
            throw new ConfigurationException("Error loading configuration. [properties=" + propf + "]", e);
        }
    }
}
