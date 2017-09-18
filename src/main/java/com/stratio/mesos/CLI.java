/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos;

import com.stratio.mesos.api.ExhibitorApi;
import com.stratio.mesos.api.MarathonApi;
import com.stratio.mesos.api.MesosApi;

import java.util.Arrays;

/**
 * Created by alonso on 30/06/17.
 */
public class CLI {

    // mesos resources unreserve
    public static void unreserve(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        String[] frameworkIds = findFrameworkIds(mesos, principal, role, serviceName, active);
        println("Found " + frameworkIds.length + " frameworks");
        for (String frameworkId : frameworkIds) {
            unreserve(mesos, frameworkId, role);
        }
    }

    // mesos resources unreserve
    public static void unreserve(MesosApi mesos, String frameworkId, String role) {
        Arrays.stream(
                mesos.findSlavesForFramework(frameworkId).orElse(new String[]{}))
                .forEach(slaveId -> {
                    System.out.println("Looking for resources on slave " + slaveId);
                    String[] resources = mesos.findResourcesFor(role, slaveId);
                    for (String resource : resources) {
                        int code = mesos.unreserveResourceFor(slaveId, resource);
                        System.out.println("Janithor " + resource + ": " + code);
                    }
                });
    }

    // Mesos framework teardown
    public static void teardown(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        if (role==null) {
            boolean teardown = mesos.teardown(serviceName);
            println("Teardown "+serviceName+" returned " + teardown);
        } else {
            String[] frameworkIds = findFrameworkIds(mesos, principal, role, serviceName, active);
            println("Found " + frameworkIds.length + " frameworks");
            for (String frameworkId : frameworkIds) {
                boolean teardown = mesos.teardown(frameworkId);
                println("Teardown "+frameworkId+" returned " + teardown);
            }
        }
    }

    // Marathon service destroy
    public static void destroy(MarathonApi marathon, String serviceName) {
        boolean destroy = marathon.destroy(serviceName);
        System.out.println("Marathon service "+serviceName+" shutdown is " + destroy);
    }

    // transform ppal, role and framework into FrameworkId
    public static void lookup(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        String[] frameworkIds = mesos.findFrameworkId(serviceName, role, principal, active).orElse(new String[]{});
        for (String frameworkId : frameworkIds) {
            println(frameworkId);
        }
    }

    // list all resources
    public static void resources(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        String[] frameworkIds = findFrameworkIds(mesos, principal, role, serviceName, active);
        println("Found " + frameworkIds.length + " frameworks");

        // resources unreserve
        for (String frameworkId : frameworkIds) {
            Arrays.stream(
                    mesos.findSlavesForFramework(frameworkId).orElse(new String[]{}))
                    .forEach(slaveId -> {
                        println("\nResources on slave " + slaveId);
                        String[] resources = mesos.findResourcesFor(role, slaveId);
                        for (String resource : resources) {
                            println(resource);
                        }
                    });
        }
    }

    // exhibitor znode cleanup
    public static void cleanup(ExhibitorApi exhibitor, String serviceName) {
        boolean deleted = exhibitor.delete(serviceName);
        println("Service " + serviceName + " was " + (deleted?"deleted successfully":"not deleted"));
    }

    public static String dcosToken(String url, String user, String pass) {
        String token = MarathonApi.obtainToken(user, pass, url + "/login?firstUser=false");
        println(token);
        return token;
    }

    private static String[] findFrameworkIds(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        String[] frameworkIds;
        if (role==null) {
            frameworkIds = new String[]{serviceName};
        } else {
            frameworkIds = mesos.findFrameworkId(serviceName, role, principal, active).orElse(new String[]{});
        }

        return frameworkIds;
    }

    // TODO: add proper log
    private static void println(String message) {
        System.out.println(message);
    }
}
