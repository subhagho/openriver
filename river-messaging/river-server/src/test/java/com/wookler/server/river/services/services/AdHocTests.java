package com.wookler.server.river.services.services;

import com.wookler.server.common.config.Config;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigPath;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created re10/08/14
 */
public class AdHocTests {

    public static void main(String[] args) {
        try {
            String cf = "/work/dev/RIVER/server/src/test/resources/server-config-pull.xml";
            Config c = new Config(cf, "/configuration");
            c.load();

            ConfigNode n = c.node();
            if (n instanceof ConfigPath) {
                ConfigPath cp = (ConfigPath) n;
                ConfigNode cn = cp.search("services.publisher");
                if (cn == null) {
                    throw new Exception("Not found...");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
