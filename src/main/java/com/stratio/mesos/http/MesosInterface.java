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
import retrofit2.http.*;

/**
 * Created by jmartinez on 7/02/17.
 */
public interface MesosInterface {

    @FormUrlEncoded
    @POST("/{endpointPrefix}/unreserve")
    Call<ResponseBody> unreserve(
            @Path("endpointPrefix") String endpointPrefix,
            @Field("slaveId") String slaveId,
            @Field("resources") String resources
    );

    @FormUrlEncoded
    @POST("/unreserve")
    Call<ResponseBody> unreserve(
            @Field("slaveId") String slaveId,
            @Field("resources") String resources
    );

    @FormUrlEncoded
    @POST("/{endpointPrefix}/destroy-volumes")
    Call<ResponseBody> destroyVolumes(
            @Path("endpointPrefix") String endpointPrefix,
            @Field("slaveId") String slaveId,
            @Field("volumes") String volumes
    );

    @FormUrlEncoded
    @POST("/destroy-volumes")
    Call<ResponseBody> destroyVolumes(
            @Field("slaveId") String slaveId,
            @Field("volumes") String volumes
    );


    @GET("/{endpointPrefix}/slaves")
    Call<ResponseBody> findResources(
            @Path("endpointPrefix") String endpointPrefix
    );

    @GET("/slaves")
    Call<ResponseBody> findResources();

    @GET("/{endpointPrefix}/frameworks")
    Call<ResponseBody> findFrameworks(
            @Path("endpointPrefix") String endpointPrefix
    );

    @GET("/frameworks")
    Call<ResponseBody> findFrameworks();

    @FormUrlEncoded
    @POST("/{endpointPrefix}/teardown")
    Call<ResponseBody> teardown(
            @Path("endpointPrefix") String endpointPrefix,
            @Field("frameworkId") String frameworkId
    );

    @FormUrlEncoded
    @POST("/teardown")
    Call<ResponseBody> teardown(
            @Field("frameworkId") String frameworkId
    );

}