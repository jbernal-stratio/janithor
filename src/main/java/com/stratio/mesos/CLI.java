/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.mesos.api.ExhibitorApi;
import com.stratio.mesos.api.MarathonApi;
import com.stratio.mesos.api.MesosApi;
import net.thisptr.jackson.jq.JsonQuery;

import javax.net.ssl.*;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by alonso on 30/06/17.
 */
public class CLI {

    // mesos resources unreserve
    public static List<String> unreserve(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        List<String> returnCodes = new ArrayList<>();
        String[] frameworkIds = findFrameworkIds(mesos, principal, role, serviceName, active);
        println("Found " + frameworkIds.length + " frameworks");
        for (String frameworkId : frameworkIds) {
            returnCodes.addAll(unreserve(mesos, frameworkId, role));
        }
        return returnCodes;
    }

    // mesos resources unreserve
    public static List<String> unreserve(MesosApi mesos, String frameworkId, String role) {
        List<String> returnCodes = new ArrayList<>();
        Arrays.stream(
                mesos.findSlavesForFramework(frameworkId).orElse(new String[]{}))
                .forEach(slaveId -> {
                    System.out.println("Looking for resources on slave " + slaveId);
                    String[] resources = mesos.findResourcesFor(role, slaveId);
                    for (String resource : resources) {
                        int code = mesos.unreserveResourceFor(slaveId, resource);
                        System.out.println("Janithor " + resource + ": " + code);
                        returnCodes.add("[" + code + "] - " + resource);
                    }
                });
        return returnCodes;
    }

    // dracarys: mesos resources unreserve just by role
    public static void unreserve(MesosApi mesos, String role) {
        String[] slaveIds = mesos.findAllSlaves().orElse(new String[]{});
        for (String slaveId : slaveIds) {
            println("\nResources on slave " + slaveId);
            String[] resources = mesos.findResourcesFor(role, slaveId);
            for (String resource : resources) {
                mesos.unreserveResourceFor(slaveId, resource);
            }
        }
    }

    // Mesos framework teardown
    public static boolean teardown(MesosApi mesos, String principal, String role, String serviceName, boolean active) {
        boolean teardown = false;
        if (role==null) {
            teardown = mesos.teardown(serviceName);
            println("Teardown "+serviceName+" returned " + teardown);
        } else {
            String[] frameworkIds = findFrameworkIds(mesos, principal, role, serviceName, active);
            println("Found " + frameworkIds.length + " frameworks");
            for (String frameworkId : frameworkIds) {
                teardown &= mesos.teardown(frameworkId);
                println("Teardown "+frameworkId+" returned " + teardown);
            }
        }
        return teardown;
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

    public static String dcosToken(String url, String tenant, String user, String pass) {
        String token = MarathonApi.obtainToken(tenant, user, pass, url + "/login?firstUser=false");
        println(token);
        return token;
    }

    public static String findMesosSecret(String vaultUrl, String vaultToken) {
        ObjectMapper MAPPER = new ObjectMapper();

        try {
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            HostnameVerifier allHostsValid = (hostname, session) -> true;

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
            URL url = new URL(vaultUrl);
            URLConnection con = url.openConnection();
            con.addRequestProperty("X-Vault-Token", vaultToken);
            Reader reader = new InputStreamReader(con.getInputStream());
            String response = "";
            while (true) {
                int ch = reader.read();
                if (ch == -1) {
                    break;
                }
                response += (char) ch;
            }

            JsonQuery q = JsonQuery.compile("\"\\(.data.user):\\(.data.pass)\"");
            JsonNode in = MAPPER.readTree(new String(response.getBytes()));
            List<JsonNode> resources = q.apply(in);

            return resources.stream()
                    .map(list -> list.toString().replace("\"", ""))
                    .findFirst()
                    .orElse(null);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
