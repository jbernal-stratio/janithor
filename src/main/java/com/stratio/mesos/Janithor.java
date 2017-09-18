/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos;

import com.stratio.mesos.api.ApiBuilder;
import com.stratio.mesos.api.ExhibitorApi;
import com.stratio.mesos.api.MarathonApi;
import com.stratio.mesos.api.MesosApi;
import org.apache.commons.cli.*;

/**
 * Created by alonso on 26/06/17.
 */
public class Janithor {

    public static void main(String[] args) {
        CommandLineParser parser;
        CommandLine cmd;

        Options options = new Options();
        options.addOption("o", "operation", true, "Operation to perform (lookup, teardown, unreserve, resources, exhibitor, marathon, token)");
        options.addOption("u", "url", true, "Mesos master url");
        options.addOption("p", "principal", true, "Principal");
        options.addOption("r", "role", true, "Role");
        options.addOption("f", "framework", true, "Framework name");
        options.addOption("a", true, "Active/inactive frameworks (default true)");
        options.addOption("t", true, "Token based authentication");
        options.addOption("s", true, "Secret based authentication");
        options.addOption("sso", true, "Automatic sso authentication (user:pass)");
        options.addOption("x", true, "URL prefix (mesos, master)");
        options.addOption("h", false, "Show help");

        parser = new BasicParser();
        try {
            cmd = parser.parse(options, args);
            if (cmd.getOptions().length == 0 || cmd.hasOption("h")) {
                new HelpFormatter().printHelp(Janithor.class.getCanonicalName(), options);
                return;
            }

            String url = cmd.getOptionValue("u");
            String principal = cmd.getOptionValue("p");
            String role = cmd.getOptionValue("r");
            String serviceName = cmd.getOptionValue("f");
            boolean active = true;

            // active/inactive flag for frameworks
            if (cmd.hasOption("a")) {
                active = Boolean.valueOf(cmd.getOptionValue("a"));
            }

            // automatic sso authentication
            String[] sso = new String[]{"", ""};
            if (cmd.hasOption("sso")) {
                sso = cmd.getOptionValue("sso").split(":");
            }

            MesosApi mesos = buildApi(cmd, url, principal, MesosApi.class);
            ExhibitorApi exhibitor = buildApi(cmd, url, principal, ExhibitorApi.class);
            MarathonApi marathon = buildApi(cmd, url, principal, MarathonApi.class);

            String operation = cmd.getOptionValue("o");
            switch (operation) {
                case "lookup":
                    CLI.lookup(mesos, principal, role, serviceName, active);
                    break;
                case "teardown":
                    CLI.teardown(mesos, principal, role, serviceName, active);
                    break;
                case "unreserve":
                    CLI.unreserve(mesos, principal, role, serviceName, active);
                    break;
                case "resources":
                    CLI.resources(mesos, principal, role, serviceName, active);
                    break;
                case "exhibitor":
                    CLI.cleanup(exhibitor, serviceName);
                    break;
                case "marathon":
                    CLI.destroy(marathon, serviceName);
                    break;
                case "token":
                    CLI.dcosToken(url, sso[0], sso[1]);
                    break;
            }

        } catch (ParseException e) {
            System.out.println("Missing required options");
            new HelpFormatter().printHelp(Janithor.class.getCanonicalName(), options);
        }
    }

    private static <T> T buildApi(CommandLine cmd, String url, String principal, Class<T> client) {
        String authentication;
        T api;

        // token based authentication
        if (cmd.hasOption("t")) {
            authentication = cmd.getOptionValue("t");
            api = ApiBuilder.build(authentication, url, client);
            // secret based authentication
        } else if (cmd.hasOption("s")) {
            authentication = cmd.getOptionValue("s");
            api = ApiBuilder.build(principal, authentication, url, client);
            // no authentication
        } else {
            api = ApiBuilder.build(url, client);
        }

        // MesosAPi only: Endpoint setup
        if (client.isAssignableFrom(MesosApi.class)) {
            if (cmd.hasOption("x")) {
                ((MesosApi) api).setEndpointsPrefix(MesosApi.EndpointPrefix.valueOf(cmd.getOptionValue("x").toUpperCase()));
            } else {
                ((MesosApi) api).setEndpointsPrefix(MesosApi.EndpointPrefix.EMPTY);
            }
        }

        return api;
    }
}