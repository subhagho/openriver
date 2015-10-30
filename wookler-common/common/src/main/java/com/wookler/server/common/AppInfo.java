/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.common;

/**
 * Class containing information about the running application.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 10/08/14
 */
public class AppInfo {
    private String app;
    private String ip;
    private String hostname;
    private long starttime;

    /**
     * Set the IP Address for the current host.
     *
     * @param ip - IP Address
     * @return - self.
     */
    public AppInfo ip(String ip) {
        this.ip = ip;

        return this;
    }

    /**
     * Get the IP Address of the current host.
     *
     * @return - IP Address string.
     */
    public String ip() {
        return ip;
    }

    /**
     * Set the current application name.
     *
     * @param app - Application name.
     * @return - self.
     */
    public AppInfo app(String app) {
        this.app = app;

        return this;
    }

    /**
     * Get the application name.
     *
     * @return - Application name.
     */
    public String app() {
        return app;
    }

    /**
     * Set the current hostname.
     *
     * @param hostname - Current hostname.
     * @return - self.
     */
    public AppInfo hostname(String hostname) {
        this.hostname = hostname;

        return this;
    }

    /**
     * Get the current hostname.
     *
     * @return - Hostname.
     */
    public String hostname() {
        return hostname;
    }

    /**
     * Set the application create time.
     *
     * @param starttime - Start timestamp.
     * @return - self.
     */
    public AppInfo starttime(long starttime) {
        this.starttime = starttime;

        return this;
    }

    /**
     * Get the application create time.
     *
     * @return - Application create time.
     */
    public long starttime() {
        return starttime;
    }

    /**
     * String representation of this application info.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        return String.format("APP INFO: [APP NAME:%s][IP:%s][HOSTNAME:%s][START TIME:%d]", app, ip, hostname,
                                    starttime);
    }
}
