package com.wookler.server.common.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.EObjectState;
import com.wookler.server.common.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Copyright 2017 Subho Ghosh (subho.ghosh at outlook dot com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Created By : subho
// Created On : 01/12/17

/**
 * Implementation of a configuration parser that reads the configuration from an
 * XML file.
 */
public class XMLConfigParser implements ConfigParser {
    public static final class Constants {
        public static final String CONFIG_NODE_PARAMS = "params";
        public static final String CONFIG_NODE_PARAM = "param";
        public static final String CONFIG_ATTR_NAME = "name";
        public static final String CONFIG_ATTR_VALUE = "value";
    }

    private void params(ConfigParams node, Element elm) {
        for (int ii = 0; ii < elm.getChildNodes().getLength(); ii++) {
            Node n = elm.getChildNodes().item(ii);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                Element e = (Element) n;
                if (e.getNodeName().compareToIgnoreCase(
                        ConfigParams.Constants.NODE_VALUE_NAME) == 0) {
                    String name = e
                            .getAttribute(ConfigParams.Constants.NODE_ATTR_NAME);
                    String value = e
                            .getAttribute(ConfigParams.Constants.NODE_ATTR_VALUE);
                    if (!StringUtils.isEmpty(name)
                            && !StringUtils.isEmpty(value)) {
                        node.param(name, value);
                    }
                }
            }
        }
    }

    private ConfigPath readConfigFromXML(Config root, Config config, ConfigNode parent, Element elm)
            throws ConfigurationException {
        if (root == null)
            root = config;

        if (parent instanceof ConfigPath) {
            // Check if there are any attributes.
            if (elm.hasAttributes()) {
                NamedNodeMap map = elm.getAttributes();
                if (map.getLength() > 0) {
                    ConfigAttributes attrs = new ConfigAttributes(parent, config);
                    for (int ii = 0; ii < map.getLength(); ii++) {
                        Node n = map.item(ii);
                        attrs.attribute(n.getNodeName(), n.getNodeValue());
                    }
                    ((ConfigPath) parent).attributes(attrs);
                }
            }

            if (elm.hasChildNodes()) {
                NodeList children = elm.getChildNodes();
                for (int ii = 0; ii < children.getLength(); ii++) {
                    Node cn = children.item(ii);
                    if (cn.getNodeType() == Node.ELEMENT_NODE) {
                        Element e = (Element) cn;
                        if (e.hasChildNodes()) {
                            int nc = 0;
                            for (int jj = 0; jj < e.getChildNodes().getLength(); jj++) {
                                Node ccn = e.getChildNodes().item(jj);
                                // Read the text node if there is any.
                                if (ccn.getNodeType() == Node.TEXT_NODE) {
                                    String n = e.getNodeName();
                                    String v = ccn.getNodeValue();
                                    if (!StringUtils.isEmpty(v.trim())) {
                                        ((ConfigPath) parent).valuenode(n, v);
                                        nc++;
                                    }
                                }
                            }
                            // Make sure this in not a text only node.
                            if ((nc == 0) || e.getChildNodes().getLength() > nc) {
                                // Check if this is a parameter node. Parameters
                                // are treated differently.
                                if (e.getNodeName().compareToIgnoreCase(
                                        ConfigParams.Constants.NODE_NAME) == 0) {
                                    ConfigParams cp = ((ConfigPath) parent)
                                            .parentnode();
                                    params(cp, e);
                                } else {
                                    ConfigPath cp = ((ConfigPath) parent)
                                            .pathnode(e.getNodeName());
                                    readConfigFromXML(root, config, cp, e);
                                }
                            }
                        } else if (e.hasAttributes()) {
                            if (e.getNodeName().compareToIgnoreCase(
                                    Config.Constants.NODE_NAME_INCLUDE) == 0) {

                                String file = e
                                        .getAttribute(Config.Constants.NODE_ATTR_CONFIG_FILE);
                                if (StringUtils.isEmpty(file))
                                    throw new ConfigurationException(
                                            "Invalid include specification. Missing attribute. [attribute="
                                                    + Config.Constants.NODE_ATTR_CONFIG_FILE
                                                    + "]");
                                String rp = e
                                        .getAttribute(Config.Constants.NODE_ATTR_ROOTNODE);
                                if (StringUtils.isEmpty(rp))
                                    throw new ConfigurationException(
                                            "Invalid include specification. Missing attribute. [attribute="
                                                    + Config.Constants.NODE_ATTR_ROOTNODE
                                                    + "]");
                                Config c = new Config(file, rp);
                                this.parse(c, file, rp);
                                c.node().move(parent, config);
                                config.linkConfig(c);
                                root.linkPropertySet(c);
                            } else {
                                ConfigPath cp = ((ConfigPath) parent)
                                        .pathnode(e.getNodeName());
                                readConfigFromXML(root, config, cp, e);
                            }
                        }
                    }
                }
            }
        }
        return (ConfigPath) parent;
    }

    /**
     * Load the configuration from the path and file specified.
     *
     * @throws ConfigurationException
     */
    private void readConfigFromXML(Config config, String filename, String path) throws ConfigurationException {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(filename);

            // optional, but recommended
            // read this -
            // http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize();

            XPath xp = XPathFactory.newInstance().newXPath();
            Element root = (Element) xp.compile(path).evaluate(doc,
                    XPathConstants.NODE);
            if (root == null) {
                throw new ConfigurationException(
                        "Cannot find specified path in document. [path="
                                + path + "]");
            }

            config.create(root.getNodeName());
            ConfigNode node = config.node();
            readConfigFromXML(null, config, node, root);
            ((ConfigPath) node).finalize();
            config.finalize();

        } catch (ParserConfigurationException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        } catch (IOException ioe) {
            throw new ConfigurationException(
                    "Error reading configuration file [path=" + filename + "]",
                    ioe);
        } catch (SAXException se) {
            throw new ConfigurationException(
                    "Error parsing document [document=" + filename + "]", se);
        } catch (XPathExpressionException xpe) {
            throw new ConfigurationException(
                    "Error parsing specified XPath expression.", xpe);
        }
    }

    private void createConfiguration(Document doc, Element elem, ConfigNode node, ESaveMode mode) throws ConfigurationException {
        if (node instanceof ConfigIncludedPath) {
            ConfigIncludedPath cp = (ConfigIncludedPath) node;
            String file = cp.getIncludeFilePath();
            String path = cp.getConfigRoot();
            save(cp, file, path, mode);
        } else if (node instanceof ConfigPath) {
            ConfigPath cp = (ConfigPath) node;
            Element e = doc.createElement(cp.name());
            Map<String, ConfigNode> nodes = cp.nodes();
            if (nodes != null && !nodes.isEmpty()) {
                for (String nn : nodes.keySet()) {
                    ConfigNode cn = nodes.get(nn);
                    createConfiguration(doc, e, cn, mode);
                }
            }
            elem.appendChild(e);
        } else if (node instanceof ConfigValue) {
            ConfigValue cv = (ConfigValue) node;
            String n = cv.name();
            String v = cv.value();
            Element e = doc.createElement(n);
            e.setNodeValue(v);
            elem.appendChild(e);
        } else if (node instanceof ConfigValueList) {
            ConfigValueList cvl = (ConfigValueList) node;
            List<ConfigNode> nodes = cvl.values();
            if (nodes != null && !nodes.isEmpty()) {
                for (ConfigNode cn : nodes) {
                    createConfiguration(doc, elem, cn, mode);
                }
            }
        } else if (node instanceof ConfigParams) {
            ConfigParams params = (ConfigParams) node;
            if (params != null) {
                Map<String, String> ps = params.params();
                if (ps != null && !ps.isEmpty()) {
                    Element pes = doc.createElement(Constants.CONFIG_NODE_PARAMS);

                    for (String pk : ps.keySet()) {
                        String pv = ps.get(pk);
                        Element pe = doc.createElement(Constants.CONFIG_NODE_PARAM);
                        pe.setAttribute(Constants.CONFIG_ATTR_NAME, pk);
                        pe.setAttribute(Constants.CONFIG_ATTR_VALUE, pv);
                        pes.appendChild(pe);
                    }
                    elem.appendChild(pes);
                }
            }
        }
    }

    private void writeConfigToXML(ConfigNode node, String filename, String path, ESaveMode mode) throws ConfigurationException {
        Preconditions.checkArgument(node != null);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            // root elements
            Document doc = db.newDocument();
            String[] pnodes = path.split("/");
            Element elem = null;
            for (String pnode : pnodes) {
                if (Strings.isNullOrEmpty(pnode)) {
                    continue;
                }
                if (elem == null) {
                    elem = doc.createElement(pnode);
                    doc.appendChild(elem);
                } else {
                    Element e = doc.createElement(pnode);
                    elem.appendChild(e);
                    elem = e;
                }
            }
            if (elem == null)
                throw new ConfigurationException("Invalid configuration path specified. [path=" + path + "]");
            if (mode != ESaveMode.SAVE_ALL_TO_ROOT) {
                Set<String> props = config.properties();
                if (props != null && !props.isEmpty()) {
                    Element pe = doc.createElement(Config.ConfigProperties.NODE_NAME_PROP);
                    for (String p : props) {
                        Element ppe = doc.createElement(p);
                        String v = config.property(p);
                        if (!Strings.isNullOrEmpty(v)) {
                            ppe.setNodeValue(v);
                            pe.appendChild(ppe);
                        }
                    }
                    elem.appendChild(pe);
                }
            } else {
                Map<String, String> props = config.getAllProperties();
                if (props != null && !props.isEmpty()) {
                    Element pe = doc.createElement(Config.ConfigProperties.NODE_NAME_PROP);
                    for (String p : props.keySet()) {
                        Element ppe = doc.createElement(p);
                        String v = config.property(p);
                        if (!Strings.isNullOrEmpty(v)) {
                            ppe.setNodeValue(v);
                            pe.appendChild(ppe);
                        }
                    }
                    elem.appendChild(pe);
                }
            }
            createConfiguration(doc, elem, config.node(), mode);


            // write the content into xml file
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(filename));

            transformer.transform(source, result);

            LogUtils.mesg(getClass(), "Written configuration to file. [file=" + filename + "]");
        } catch (ParserConfigurationException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        } catch (TransformerConfigurationException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        } catch (TransformerException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        }
    }

    private void writeConfigToXML(Config config, String filename, String path, ESaveMode mode) throws ConfigurationException {
        Preconditions.checkArgument(config != null);
        Preconditions.checkArgument(config.state() == EObjectState.Available);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            // root elements
            Document doc = db.newDocument();
            String[] pnodes = path.split("/");
            Element elem = null;
            for (String pnode : pnodes) {
                if (Strings.isNullOrEmpty(pnode)) {
                    continue;
                }
                if (elem == null) {
                    elem = doc.createElement(pnode);
                    doc.appendChild(elem);
                } else {
                    Element e = doc.createElement(pnode);
                    elem.appendChild(e);
                    elem = e;
                }
            }
            if (elem == null)
                throw new ConfigurationException("Invalid configuration path specified. [path=" + path + "]");
            if (mode != ESaveMode.SAVE_ALL_TO_ROOT) {
                Set<String> props = config.properties();
                if (props != null && !props.isEmpty()) {
                    Element pe = doc.createElement(Config.ConfigProperties.NODE_NAME_PROP);
                    for (String p : props) {
                        Element ppe = doc.createElement(p);
                        String v = config.property(p);
                        if (!Strings.isNullOrEmpty(v)) {
                            ppe.setNodeValue(v);
                            pe.appendChild(ppe);
                        }
                    }
                    elem.appendChild(pe);
                }
            } else {
                Map<String, String> props = config.getAllProperties();
                if (props != null && !props.isEmpty()) {
                    Element pe = doc.createElement(Config.ConfigProperties.NODE_NAME_PROP);
                    for (String p : props.keySet()) {
                        Element ppe = doc.createElement(p);
                        String v = config.property(p);
                        if (!Strings.isNullOrEmpty(v)) {
                            ppe.setNodeValue(v);
                            pe.appendChild(ppe);
                        }
                    }
                    elem.appendChild(pe);
                }
            }
            createConfiguration(doc, elem, config.node(), mode);


            // write the content into xml file
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(new File(filename));

            transformer.transform(source, result);

            LogUtils.mesg(getClass(), "Written configuration to file. [file=" + filename + "]");
        } catch (ParserConfigurationException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        } catch (TransformerConfigurationException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        } catch (TransformerException pse) {
            throw new ConfigurationException(
                    "Error building the configuration document.", pse);
        }
    }

    @Override
    public void parse(Config config, String filename, String path) throws ConfigurationException {
        readConfigFromXML(config, filename, path);
    }

    @Override
    public void save(Config node, String filename, String path, ESaveMode mode) throws ConfigurationException {
        writeConfigToXML(node, filename, path, mode);
    }
}
