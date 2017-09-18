/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.http;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.DELETE;
import retrofit2.http.Header;
import retrofit2.http.Path;

/**
 * Created by jmartinez on 2/02/17.
 */

public interface MarathonInterface {

    @DELETE("v2/apps/{serviceName}")
    Call<ResponseBody> destroy(@Path("serviceName") String serviceName);

    @DELETE("v2/apps/{serviceName}")
    Call<ResponseBody> destroy(@Header("Cookie") String dcosCookie, @Path("serviceName") String serviceName);

}
