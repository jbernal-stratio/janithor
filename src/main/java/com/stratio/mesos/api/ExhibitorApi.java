/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.api;

import com.stratio.mesos.http.HTTPUtils;
import com.stratio.mesos.http.ExhibitorInterface;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;

/**
 * Created by alonso on 23/06/17.
 */
public class ExhibitorApi {
    private static final Logger LOG = LoggerFactory.getLogger(ExhibitorApi.class);
    
    private ExhibitorInterface exhibitorInterface;

    public ExhibitorApi(String exhibitorUrl) {
        this.exhibitorInterface = HTTPUtils.buildBasicInterface(exhibitorUrl, ExhibitorInterface.class);
    }

    public ExhibitorApi(String accessToken, String exhibitorUrl) {
        this.exhibitorInterface = HTTPUtils.buildTokenBasedInterface(accessToken, exhibitorUrl, ExhibitorInterface.class);
    }

    public ExhibitorApi(String principal, String secret, String exhibitorUrl) {
        this.exhibitorInterface = HTTPUtils.buildSecretBasedInterface(principal, secret, exhibitorUrl, ExhibitorInterface.class);
    }

    /**
     * Removes an entry named serviceName from exhibitor
     * @param serviceName
     * @return
     */
    public boolean delete(String serviceName) {
        Call<ResponseBody> mesosCall;
        try {
            mesosCall = exhibitorInterface.delete(serviceName);
            Response<ResponseBody> response = mesosCall.execute();
            LOG.info(response.message());
            return (response.code() == HTTPUtils.HTTP_OK_CODE);
        } catch (IOException e) {
            LOG.info("Exhibitor failure with message " + e.getMessage());
            return false;
        }
    }
}
