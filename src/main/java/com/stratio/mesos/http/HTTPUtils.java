/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.http;

import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;

/**
 * Created by alonso on 23/06/17.
 */
public class HTTPUtils {
    public static final int HTTP_OK_CODE = 200;
    public static final int UNRESERVE_OK_CODE = 202;

    /**
     * Builds an unauthenticated REST interface to access mesos
     * @return the REST interface
     */
    public static <T> T buildBasicInterface(String baseUrl, Class<T> serverInterface) {
        Retrofit mesosInterfaceBuilder = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .client(getUnsafeOkHttpClient().build())
                .build();

        return mesosInterfaceBuilder.create(serverInterface);
    }

    /**
     * Builds an authenticated REST interface using a dcos token
     * @param token DC/OS Authorization token
     * @return the authenticated REST interface
     */
    public static <T> T buildTokenBasedInterface(String token, String baseUrl, Class<T> serverInterface) {
        Retrofit mesosInterfaceBuilder = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .client(getUnsafeOkHttpClient("Authorization", "token="+token).build())
                .build();

        return mesosInterfaceBuilder.create(serverInterface);
    }

    /**
     * Builds an authenticated REST interface using a dcos token passed with dcos-acs-auth-cookie
     * @param token DC/OS Authorization token
     * @return the authenticated REST interface
     */
    public static <T> T buildCookieBasedInterface(String token, String baseUrl, Class<T> serverInterface) {
        Retrofit mesosInterfaceBuilder = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .client(getUnsafeOkHttpClient("Cookie", "dcos-acs-auth-cookie="+token).build())
                .build();

        return mesosInterfaceBuilder.create(serverInterface);
    }

    /**
     * Builds an authenticated REST interface using mesos secret
     * @param principal mesos principal
     * @param secret mesos secret
     * @return the authenticated REST interface
     */
    public static <T> T buildSecretBasedInterface(String principal, String secret, String baseUrl, Class<T> serverInterface) {
        Retrofit mesosInterfaceBuilder = new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .client(getUnsafeOkHttpClient("Authorization", Credentials.basic(principal, secret)).build())
                .build();

        return mesosInterfaceBuilder.create(serverInterface);
    }

    /**
     * Builds a HTTP client using the specified token as Authorization header, ignoring all server certificates in the process
     * @return HTTP client (no client SSL verification)
     */
    public static OkHttpClient.Builder getUnsafeOkHttpClient() {
        return getUnsafeOkHttpClient(null, null);
    }

    /**
     * Builds a HTTP client using the specified token as Authorization header, ignoring all server certificates in the process
     * @param token DC/OS token authentication
     * @return HTTP client (no client SSL verification)
     */
    public static OkHttpClient.Builder getUnsafeOkHttpClient(String header, String token) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        try {
            if (token!=null) {
                Interceptor mTokenInterceptor = chain -> {
                    Request request = chain.request();
                    if (token != null) {
                        Request.Builder requestBuilder = request.newBuilder()
                                .addHeader(header, token);
                        Request newRequest = requestBuilder.build();

                        return chain.proceed(newRequest);
                    }
                    return chain.proceed(request);
                };

                builder.addNetworkInterceptor(mTokenInterceptor);
            }

            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }
                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }
                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier((hostname, session) -> true);
            builder.followRedirects(true);
            builder.retryOnConnectionFailure(true);

            return builder;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
