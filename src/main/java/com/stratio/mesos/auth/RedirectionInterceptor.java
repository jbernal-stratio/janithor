/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.auth;

import okhttp3.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shernandez on 10/03/17.
 */
public class RedirectionInterceptor implements Interceptor {
    Logger LOG = LoggerFactory.getLogger(RedirectionInterceptor.class);

    List<String> locationHistory = new ArrayList<>();

    @Override
    public okhttp3.Response intercept(Chain chain) throws IOException {
        locationHistory.add(chain.request().url().toString());

        return chain.proceed(chain.request());
    }

    public void clearLocationHistory(){
        locationHistory.clear();
    }

    public List<String> getLocationHistory(){
        return locationHistory;
    }
}
