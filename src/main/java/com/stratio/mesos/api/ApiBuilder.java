/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.api;

import java.lang.reflect.Constructor;
import java.util.Arrays;

/**
 * Created by alonso on 27/06/17.
 */
public class ApiBuilder {

    public static <T> T build(String principal, String secret, String url, Class<T> client) {
        Constructor<?> constructor = findSuitableConstructor(client, 3);
        return instantiateClient(client, constructor, principal, secret, url);
    }

    public static <T> T build(String token, String url, Class<T> client) {
        Constructor<?> constructor = findSuitableConstructor(client, 2);
        return instantiateClient(client, constructor, token, url);
    }

    public static <T> T build(String url, Class<T> client) {
        Constructor<?> constructor = findSuitableConstructor(client, 1);
        return instantiateClient(client, constructor, url);
    }

    // find a constructor for the given client class with the specified argument number
    private static Constructor<?> findSuitableConstructor(Class<?> client, int argNumber) {
        Constructor<?>[] constructors = client.getDeclaredConstructors();
        Constructor<?> constructor = Arrays.stream(constructors)
                .filter(c -> c.getParameterCount() == argNumber)
                .findFirst()
                .orElse(null);
        return constructor;
    }

    // make an object out of a constructor with the arguments provided
    private static <T> T instantiateClient(Class<T> client, Constructor<?> constructor, String... args) {
        // return a new instance of the specified client
        if (constructor!=null) {
            try {
                return (T) constructor.newInstance(args);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } else {
            System.out.println("No valid constructor found for client " + client.getCanonicalName());
            return null;
        }
    }

}
