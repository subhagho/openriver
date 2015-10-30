package com.wookler.server.common.structs;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("unused")
public class ACLTest {
    private static final ACL acl = new ACL();

    @Test
    public void testToString() throws Exception {
        acl.set("g+w");
        acl.set("u+x");
        if (!acl.check("gw")) {
            throw new Exception("Test failed...");
        }
        if (acl.check("uw")) {
            throw new Exception("Test failed...");
        }
        System.out.println("ACL [g+w] = " + acl.toString());
    }
}