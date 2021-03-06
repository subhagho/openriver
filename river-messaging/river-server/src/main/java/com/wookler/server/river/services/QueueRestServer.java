/*
 * Copyright [2014] Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.river.services;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EProcessState;
import com.wookler.server.common.Env;
import com.wookler.server.common.GlobalConstants;
import com.wookler.server.common.ProcessState;
import com.wookler.server.common.StateException;
import com.wookler.server.common.config.ConfigAttributes;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.model.ServerException;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.remote.common.AbstractQueueService;
import com.wookler.server.river.services.admin.QueueJsonAdminService;

/**
 * Jetty based Queue Server. Queue server exposes Publish and Subscribe
 * end-points for applications to use the server as a messaging bridge. It also
 * provides support for exposing additional end-points (optional) and admin
 * service end-point (optional). If no explicit admin service is provided, then
 * the default one is used.
 * 
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 30/08/14
 */
public class QueueRestServer implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(QueueRestServer.class);

    @Option(name = "--config", usage = "Path to server configuration file.", required = true, aliases = { "-c" })
    private String configfile;

    @Option(name = "--root", usage = "XPath root for the configuration file.", required = false, aliases = { "-r" })
    private String rootpath;

    @Option(name = "--ssl", usage = "Use SSL mode for communication.", required = false)
    private boolean sslmode = false;

    @Option(name = "--keystore", usage = "Keystore file, if SSL mode is true.", required = false, aliases = { "-k" })
    private String keystore;

    @Option(name = "--password", usage = "Keystore password, if SSL mode is true.", required = false, aliases = { "-p" })
    private String password;

    @Option(name = "--adminpwd", usage = "Admin operation password.", required = true, aliases = { "-pa" })
    private String adminpass;

    private ProcessState state = new ProcessState();

    /** Jetty Server */
    private Server q_server;

    /** admin service */
    private QueueJsonAdminService adminService;

    /** subscriber (reader) service (optional) */
    private AbstractQueueService readerService;

    /** publisher (writer) service */
    private AbstractQueueService writerService;

    /** additional service (optional) */
    private AbstractQueueService optionalService;

    private static ServerContext ctx = new ServerContext();

    private static QueueRestServer queueServer = new QueueRestServer();

    /**
     * Configure the Queue REST Server. Sample:
     * 
     * <pre>
     * {@code
     *     <server>
     *          <params>
     *              <param name="server.queue.send.port" value="8080"/>
     *              <param name="server.queue.recv.port" value="9080"/>
     *              <param name="server.queue.admin.port" value="7080"/>
     *              <param name="server.queue.send.ssl.port" value="8443"/>
     *              <param name="server.queue.recv.ssl.port" value="9443"/>
     *              <param name="server.queue.admin.ssl.port" value="7443"/>
     *              <param name="server.queue.reader.threads" value="4"/>
     *              <param name="server.queue.writer.threads" value="2"/>
     *          </params>
     *          <queue ...>
     *          </queue>
     *          <services>
     *              <publisher class="com.wookler.server.river.services.services.TestJsonWriterService
     * package="com.wookler.server.river.services.services">
     *                  <params>
     *                      <param name="service.protocol" value="com.wookler.server.river.services.services.TestJsonProtocolHandler"/>
     *                  </params>
     *              </publisher>
     *              <subscriber class="com.wookler.server.river.services.services.TestJsonReaderService"
     * package="com.wookler.server.river.services.services">
     *                  <params>
     *                      <param name="service.protocol" value="com.wookler.server.river.services.services.TestJsonProtocolHandler"/>
     *                      <param name="service.queue.timeout" value="10000"/>
     *                      <param name="service.queue.batch.size" value="4096"/>
     *                  </params>
     *              </subscriber>
     *              
     *              <!-- optional service that can be additionally supported-->
     *              <service class="REST api impl class"
     * package="package name" name="ADDITIONAL-REST-SERVICE" path="REST SERVICE BASE PATH">
     *                  <params>
     *                      <param name="service.protocol" value="com.wookler.server.river.services.services.TestJsonProtocolHandler"/>
     *                      <param name="queue.name" value="QUEUE-NAME" />
     *                  </params>
     *              </service>
     *              
     *              <!-- optional admin that can be specified (If not specified, then default admin service will be invoked)-->
     *              <admin class="admin service impl class"
     * package="package name" name="admin service name">
     *                  <params>
     *                      <param name="" value=""/>
     *                  </params>
     *              </admin>
     *              
     *          </services>
     *     </server>
     * }
     * </pre>
     * 
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        ctx.sslmode(sslmode).configure(config);
    }

    /**
     * Dispose this instance of the Queue Server.
     */
    @Override
    public void dispose() {
        try {
            if (state.getState() != EProcessState.Exception) {
                state.setState(EProcessState.Stopped);
            }
            // stop the jetty server
            if (q_server != null) {
                q_server.stop();
            }
            ctx.dispose();
        } catch (Throwable t) {
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
        }
    }

    /**
     * Start the Queue REST Server
     * 
     * @param args
     *            program arguments
     * @throws ServerException
     */
    private void start(String[] args) throws ServerException {
        try {
            CmdLineParser cp = new CmdLineParser(this);
            cp.getProperties().withUsageWidth(120);

            try {
                cp.parseArgument(args);

                // check if config file is empty
                if (StringUtils.isEmpty(configfile)) {
                    throw new ServerException(getClass(), "Configuration file is null or empty.");
                }
                File cf = new File(configfile);
                // check if config file is readable
                if (!cf.exists() || !cf.canRead()) {
                    throw new ServerException(getClass(),
                            "Configuration file not found or not readable. [file=" + configfile
                                    + "]");
                }
                // check if rootpath is empty, use the default config root path
                if (StringUtils.isEmpty(rootpath)) {
                    rootpath = ServerContext.Constants.CONFIG_ROOT_PATH;
                }

                // initialize the environment context
                Env.create(configfile, rootpath);

                // get the config node for rootpath
                ConfigNode node = Env.get().config().search(rootpath);

                // configure the server
                configure(node);

                // start the queue(s)
                ctx.startQueues();

                // initialize the Jetty server
                q_server = new Server(ctx.q_port());
                // configure ssl mode params
                if (ctx.sslmode()) {
                    SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();
                    ssl_connector.setPort(ctx.q_sslPort());
                    SslContextFactory scf = ssl_connector.getSslContextFactory();
                    if (StringUtils.isEmpty(keystore)) {
                        throw new ServerException(getClass(),
                                "No keystore path specified for SSL mode.");
                    }
                    scf.setKeyStorePath(keystore);
                    if (StringUtils.isEmpty(password)) {
                        throw new ServerException(getClass(),
                                "No password path specified for SSL keystore.");
                    }
                    scf.setKeyStorePassword(password);
                    scf.setKeyManagerPassword(password);

                    q_server.setConnectors(new Connector[] { ssl_connector });
                }

                ContextHandlerCollection ctxs = new ContextHandlerCollection();

                q_server.setHandler(ctxs);

                // construct a handler list with an initial capacity of 3
                // (reader, writer and admin)
                List<Handler> handlers = new ArrayList<Handler>(3);
                // start the admin service
                Handler h = startadminservice(node);
                handlers.add(h);

                // Defining readers is optional
                // start the reader service
                h = startreaders(node);
                if (h != null)
                    handlers.add(h);

                // start the writer service
                h = startwriters(node);
                handlers.add(h);

                // start additional service (if any)
                h = startoptservice(node);
                if (h != null)
                    handlers.add(h);

                Handler[] harray = new Handler[handlers.size()];
                for (int ii = 0; ii < handlers.size(); ii++) {
                    harray[ii] = handlers.get(ii);
                }

                ctxs.setHandlers(harray);

                QueuedThreadPool qtp = new QueuedThreadPool(ctx.n_threads());
                qtp.setName("QueueServer-JETTY");
                q_server.setThreadPool(qtp);

                q_server.start();

                // set the process state to RUNNING
                state.setState(EProcessState.Running);

                q_server.join();

            } catch (CmdLineException e) {
                System.err.println(String.format("Usage:\n %s %s", getClass().getCanonicalName(),
                        cp.printExample(OptionHandlerFilter.ALL)));
                throw e;
            }
        } catch (Throwable t) {
            exception(t);
            LogUtils.error(getClass(), t, log);
            LogUtils.stacktrace(getClass(), t, log);
            throw new ServerException(getClass(), "Error starting server.", t);
        }
    }

    /**
     * Register the service with the given context path and package, with the
     * context handler and return the handler.
     * 
     * @param path
     *            service path
     * @param pack
     *            service package
     * @return context handler corresponding to the service
     * @throws Exception
     */
    private Handler registerService(String path, String pack) throws Exception {
        LogUtils.debug(getClass(), String.format("[PATH:%s][PACKAGE:%s]", path, pack));

        Map<String, Object> m_init = new HashMap<String, Object>();

        m_init.put("com.sun.jersey.api.json.POJOMappingFeature", "true");

        m_init.put("com.sun.jersey.config.property.packages", "com.fasterxml.jackson.jaxrs;" + pack);

        m_init.put("com.sun.jersey.config.property.resourceConfigClass",
                "com.sun.jersey.api.core.PackagesResourceConfig");
        if (ctx.compress()) {
            m_init.put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
                    com.sun.jersey.api.container.filter.GZIPContentEncodingFilter.class.getName());
        }
        ServletHolder sh = new ServletHolder(new ServletContainer(
                new PackagesResourceConfig(m_init)));
        ServletContextHandler restctx = new ServletContextHandler(ServletContextHandler.SESSIONS);
        restctx.setContextPath(path);
        restctx.addServlet(sh, "/*");

        return restctx;
    }

    /**
     * Configure and start the publisher service. Return a handler to the
     * publisher service after registration with the context handler
     * 
     * @param config
     *            root configuration node containing publisher element
     * @return publisher service handler
     * 
     * @throws ServerException
     */
    private Handler startwriters(ConfigNode config) throws ServerException {
        try {
            if (!(config instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            }
            ConfigPath cp = (ConfigPath) config;
            LogUtils.debug(getClass(), String.format("WRITER CONFIG: %s", cp.toString()));
            ConfigNode node = cp.search(AbstractQueueService
                    .servicePath(AbstractQueueService.Constants.CONFIG_SERVICE_WRITER));
            // publisher service is mandatory
            if (node == null) {
                throw new ConfigurationException("No publisher service specified. [node="
                        + cp.toString() + "]");
            }
            if (!(node instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
            }

            // Load and configure the publisher service.
            ConfigAttributes ca = ConfigUtils.attributes(node);
            if (ca == null) {
                throw new ServerException(getClass(), "Missing configuration attributes.");
            }
            String c = ca.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            if (StringUtils.isEmpty(c)) {
                throw new ConfigurationException("Missing or Invalid attribute.[name="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            }
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof AbstractQueueService)) {
                throw new ConfigurationException("Invalid Service class. [class="
                        + cls.getCanonicalName() + "]");
            }
            writerService = (AbstractQueueService) o;
            String pack = ca.attribute(AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE);
            if (StringUtils.isEmpty(pack)) {
                throw new ConfigurationException("Missing attribute. [name="
                        + AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE + "]");
            }
            writerService.configure(cp);
            writerService.start();

            return registerService("/publisher", pack);

        } catch (Throwable t) {
            throw new ServerException(getClass(), "Error starting Writer.", t);
        }
    }

    /**
     * If specified, configure and start the subscriber service. Return a
     * handler to the subscriber service after registration with the context
     * handler. Otherwise return null
     * 
     * @param config
     *            root configuration node to check for subscriber element
     * @return the subscriber service handler, if any. Otherwise return null
     * @throws ServerException
     */
    private Handler startreaders(ConfigNode config) throws ServerException {
        try {
            if (!(config instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            }
            ConfigPath cp = (ConfigPath) config;
            LogUtils.debug(getClass(), String.format("READER CONFIG: %s", cp.toString()));
            ConfigNode node = cp.search(AbstractQueueService
                    .servicePath(AbstractQueueService.Constants.CONFIG_SERVICE_READER));
            if (node == null) {
                return null;
            }

            if (!(node instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
            }

            // Load and configure the reader service.
            ConfigAttributes ca = ConfigUtils.attributes(node);
            if (ca == null) {
                throw new ServerException(getClass(), "Missing configuration attributes.");
            }
            String c = ca.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            if (StringUtils.isEmpty(c)) {
                throw new ConfigurationException("Missing or Invalid attribute.[name="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            }
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof AbstractQueueService)) {
                throw new ConfigurationException("Invalid Service class. [class="
                        + cls.getCanonicalName() + "]");
            }
            readerService = (AbstractQueueService) o;
            String pack = ca.attribute(AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE);
            if (StringUtils.isEmpty(pack)) {
                throw new ConfigurationException("Missing attribute. [name="
                        + AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE + "]");
            }

            readerService.configure(cp);
            readerService.start();
            return registerService("/subscriber", pack);

        } catch (Throwable t) {
            throw new ServerException(getClass(), "Error starting Reader.", t);
        }
    }

    /**
     * If specified, configure and start the additional service. Return a
     * handler to the service after registration with the context handler.
     * Otherwise return null.
     * 
     * Currently only one additional service is supported.
     * 
     * @param config
     *            root configuration node to check for additional service
     *            element
     * @return additional service handler, if any. Otherwise return null.
     * @throws ServerException
     */
    private Handler startoptservice(ConfigNode config) throws ServerException {
        try {
            if (!(config instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            }
            ConfigPath cp = (ConfigPath) config;
            LogUtils.debug(getClass(), String.format("SERVICE CONFIG: %s", cp.toString()));
            ConfigNode node = cp.search(AbstractQueueService
                    .servicePath(AbstractQueueService.Constants.CONFIG_SERVICE_OPTIONAL));
            if (node == null) {
                return null;
            }

            if (!(node instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
            }

            // Load and configure the optional service that is specified
            ConfigAttributes ca = ConfigUtils.attributes(node);
            if (ca == null) {
                throw new ServerException(getClass(), "Missing configuration attributes.");
            }
            String c = ca.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            if (StringUtils.isEmpty(c)) {
                throw new ConfigurationException("Missing or Invalid attribute.[name="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            }
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof AbstractQueueService)) {
                throw new ConfigurationException("Invalid Service class. [class="
                        + cls.getCanonicalName() + "]");
            }
            optionalService = (AbstractQueueService) o;
            String pack = ca.attribute(AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE);
            if (StringUtils.isEmpty(pack)) {
                throw new ConfigurationException("Missing attribute. [name="
                        + AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE + "]");
            }
            String path = ca.attribute(AbstractQueueService.Constants.CONFIG_ATTR_PATH);
            if (StringUtils.isEmpty(path)) {
                throw new ConfigurationException("Missing attribute. [name="
                        + AbstractQueueService.Constants.CONFIG_ATTR_PATH + "]");
            }

            optionalService.configure(cp);
            optionalService.start();
            return registerService(path, pack);

        } catch (Throwable t) {
            throw new ServerException(getClass(),
                    "Error starting additionally specified optional service.", t);
        }
    }

    /**
     * If specified, configure and start the admin service. Otherwise start the
     * default admin service. Return a handler to the service after registration
     * with the context handler.
     * 
     * @param config
     *            root configuration node to check for admin service element
     * @return handler corresponding to configured admin service. If not
     *         specified, the default admin service handler.
     * @throws ServerException
     */
    private Handler startadminservice(ConfigNode config) throws ServerException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            ConfigPath cp = (ConfigPath) config;
            LogUtils.debug(getClass(), String.format("SERVICE CONFIG: %s", cp.toString()));
            ConfigNode node = cp.search(AbstractQueueService
                    .servicePath(AbstractQueueService.Constants.CONFIG_SERVICE_ADMIN));
            if (node == null) {
                // start the default admin service
                return startadmin();
            }
            // start the configured admin service
            if (!(node instanceof ConfigPath)) {
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), node.getClass().getCanonicalName()));
            }

            // Load and configure the admin service that is specified
            ConfigAttributes ca = ConfigUtils.attributes(node);
            if (ca == null) {
                throw new ServerException(getClass(), "Missing configuration attributes.");
            }
            String c = ca.attribute(GlobalConstants.CONFIG_ATTR_CLASS);
            if (StringUtils.isEmpty(c)) {
                throw new ConfigurationException("Missing or Invalid attribute.[name="
                        + GlobalConstants.CONFIG_ATTR_CLASS + "]");
            }
            Class<?> cls = Class.forName(c);
            Object o = cls.newInstance();
            if (!(o instanceof QueueJsonAdminService)) {
                throw new ConfigurationException("Invalid Admin service class. [class="
                        + cls.getCanonicalName() + "]");
            }
            adminService = (QueueJsonAdminService) o;
            String pack = ca.attribute(AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE);
            if (StringUtils.isEmpty(pack)) {
                throw new ConfigurationException("Missing attribute. [name="
                        + AbstractQueueService.Constants.CONFIG_ATTR_PACKAGE + "]");
            }
            adminService.password(adminpass);
            adminService.configure(cp);
            adminService.start();
            return registerService("/admin", pack);

        } catch (Throwable t) {
            throw new ServerException(getClass(), "Error starting admin service.", t);
        }
    }

    /**
     * Start the default
     * {@link com.wookler.server.river.services.admin.QueueJsonAdminService}
     * 
     * @return admin service handler
     * @throws ServerException
     */
    private Handler startadmin() throws ServerException {
        try {

            adminService = new QueueJsonAdminService();
            adminService.password(adminpass);
            adminService.configure(null);
            adminService.start();

            return registerService("/admin", "com.wookler.server.river.services.admin");
        } catch (Throwable t) {
            throw new ServerException(getClass(), "Error starting Admin.", t);
        }
    }

    /**
     * Set the process state to exception and the corresponding exception cause
     * 
     * @param t
     *            Exception cause
     */
    private void exception(Throwable t) {
        state.setState(EProcessState.Exception).setError(t);
    }

    /**
     * Get a handle to the Server Context instance.
     * 
     * @return - Server Context instance.
     * @throws ServerException
     */
    public static ServerContext context() throws ServerException {
        try {
            ProcessState.check(queueServer.state, EProcessState.Running, QueueRestServer.class);
            return ctx;
        } catch (StateException e) {
            throw new ServerException(QueueRestServer.class, "Server in not running.", e);
        }
    }

    /**
     * Shutdown the Queue Server.
     */
    public static void shutdown() {
        queueServer.dispose();
    }

    /**
     * Start the Queue Server.
     * 
     * @param args
     *            - Program arguments.
     */
    public static void main(String[] args) {
        try {
            queueServer.start(args);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
