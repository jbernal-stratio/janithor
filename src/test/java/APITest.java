/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
import com.stratio.mesos.api.ApiBuilder;
import com.stratio.mesos.api.MesosApi;
import com.stratio.mesos.http.MesosInterface;
import okhttp3.*;
import okio.BufferedSource;
import okio.Okio;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.mock.Calls;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * Created by alonso on 21/06/17.
 */
@RunWith(MockitoJUnitRunner.class)
public class APITest {
    // kafka-sec = active framework
    private final String PRINCIPAL = "kafka-sec";
    private final String ROLE = "kafka-sec";
    private final String SERVICE = "kafka-sec";
    private final String FRAMEWORK_ID = "f84a629e-8d2a-45d7-aac7-d95a75149212-0001";
    private final String SLAVE_ID = "80f0fb53-95a0-409b-a78d-ca0e002cc289-S9";

    // elasticsearchstratio-1 = completed framework
    private final String INACTIVE_SERVICE = "elasticsearchstratio-1";
    private final String INACTIVE_PRINCIPAL = "elasticsearchstratio-1-principal";
    private final String INACTIVE_ROLE = "elasticsearchstratio-1-role";
    private final String INACTIVE_FRAMEWORK_ID = "00e334f4-e428-40a3-b830-1dc43c744809-0000";

    @Mock
    private MesosInterface mesosInterface;

    private MesosApi mesosApi;

    @Before
    public void setup() throws FileNotFoundException {
        initMocks(this);
        mesosApi = new MesosApi();

        mockMesosInterface();
    }

    private void mockMesosInterface() throws FileNotFoundException {
        mesosApi.setMesosInterface(mesosInterface);

        File fileFrameworks = new File(this.getClass().getClassLoader().getResource("frameworks_kafka_sec.json").getFile());
        // String jsonFrameworks = new java.util.Scanner(fileFrameworks,"UTF8").useDelimiter("\\Z").next();
        File fileSlaves = new File(this.getClass().getClassLoader().getResource("slaves_kafka_sec.json").getFile());
        // String jsonSlaves = new java.util.Scanner(fileSlaves,"UTF8").useDelimiter("\\Z").next();

        ResponseBody frameworksBody = new MockResponse("application/json", fileFrameworks.length(), fileFrameworks);
        Call<ResponseBody> frameworksResponse = Calls.response(frameworksBody);

        ResponseBody slavesBody = new MockResponse("application/json", fileSlaves.length(), fileSlaves);
        Call<ResponseBody> slavesResponse = Calls.response(slavesBody);

        ResponseBody noContent = new MockResponse("application/json", 0, null);
        Call<ResponseBody> emptyResponse = Calls.response(noContent);

        when(this.mesosInterface.findFrameworks()).thenReturn(frameworksResponse);
        when(this.mesosInterface.findResources()).thenReturn(slavesResponse);
        when(this.mesosInterface.unreserve(any(), any())).thenReturn(emptyResponse);
        when(this.mesosInterface.destroyVolumes(any(), any())).thenReturn(emptyResponse);
        when(this.mesosInterface.teardown(any())).thenReturn(emptyResponse);
    }

    @Test
    public void testFindFrameworkId() {
        String[] frameworkId = mesosApi.findFrameworkId(SERVICE, ROLE, PRINCIPAL)
                .orElse(null);
        Assert.assertNotNull(frameworkId);
        Assert.assertEquals(frameworkId[0], FRAMEWORK_ID);
    }

    @Test
    public void testFindCompletedFrameworkId() {
        // when no active flag is specified, the framework should be found in completed_frameworks section
        String[] frameworkId = mesosApi.findFrameworkId(INACTIVE_SERVICE, INACTIVE_ROLE, INACTIVE_PRINCIPAL)
                .orElse(null);
        Assert.assertNotNull(frameworkId);
        Assert.assertEquals(frameworkId.length, 3);
    }

    @Test
    public void testFindSlaves() {
        String[] slavesForFramework = mesosApi.findSlavesForFramework(FRAMEWORK_ID)
                .orElse(null);
        Assert.assertNotNull(slavesForFramework);
        Assert.assertTrue(slavesForFramework.length==1);
        Assert.assertTrue(slavesForFramework[0].equals(SLAVE_ID));
    }

    @Test
    public void testFindCompletedSlaves() {
        // test search on a completed_frameworks section
        String[] slavesForFramework = mesosApi.findSlavesForFramework(INACTIVE_FRAMEWORK_ID)
                .orElse(null);
        Assert.assertNotNull(slavesForFramework);
        Assert.assertEquals(slavesForFramework.length, 3);
    }

    @Test
    public void testFindResourcesForSlave() {
        String[] slavesForFramework = mesosApi.findSlavesForFramework(FRAMEWORK_ID)
                .orElse(null);
        Assert.assertNotNull(slavesForFramework);
        Assert.assertTrue(slavesForFramework.length==1);
        String[] resources = mesosApi.findResourcesFor(ROLE, slavesForFramework[0]);
        Assert.assertNotNull(resources);
        Assert.assertTrue(resources.length==7);
    }

    @Test
    public void testFindActiveFramework() {
        String[] active = mesosApi.findFrameworkId(SERVICE, ROLE, PRINCIPAL, true).orElse(null);
        String[] inactive = mesosApi.findFrameworkId(SERVICE, ROLE, PRINCIPAL, false).orElse(null);
        Assert.assertNotNull(active);
        Assert.assertNull(inactive);
    }

    @Test
    public void testFindInactiveFramework() {
        String[] inactive = mesosApi.findFrameworkId(INACTIVE_SERVICE, INACTIVE_ROLE, INACTIVE_PRINCIPAL, false).orElse(null);
        Assert.assertNotNull(inactive);
        Assert.assertTrue(inactive.length==3);
    }

    @Test
    public void testUnreserveResourceWrong() {
        int unreserveCode = mesosApi.unreserveResourceFor(SLAVE_ID, null);
        Assert.assertTrue(unreserveCode==-1);
        int volumesCode = mesosApi.unreserveVolumesFor(SLAVE_ID, null);
        Assert.assertTrue(volumesCode==-1);
        unreserveCode = mesosApi.unreserveResourceFor(SLAVE_ID, "");
        Assert.assertTrue(unreserveCode==-1);
        volumesCode = mesosApi.unreserveVolumesFor(SLAVE_ID, "");
        Assert.assertTrue(volumesCode==-1);
        unreserveCode = mesosApi.unreserveResourceFor(null, "");
        Assert.assertTrue(unreserveCode==-1);
        volumesCode = mesosApi.unreserveVolumesFor(null, "");
        Assert.assertTrue(volumesCode==-1);
    }

    @Test
    public void testUnreserveResource() {
        int unreserveCode = mesosApi.unreserveResourceFor(SLAVE_ID, "{}");
        Assert.assertTrue(unreserveCode==200);
        int volumesCode = mesosApi.unreserveVolumesFor(SLAVE_ID, "{}");
        Assert.assertTrue(volumesCode==200);
    }

    @Test
    public void testTeardown() {
        boolean teardown = mesosApi.teardown(FRAMEWORK_ID);
        Assert.assertTrue(teardown);
        teardown = mesosApi.teardown(null);
        Assert.assertFalse(teardown);
    }

    @Test
    public void testMesosApiBuilderWithToken() {
        MesosApi api = ApiBuilder.build("token", "http://leader.mesos:5050", MesosApi.class);
        Assert.assertFalse(api.hasEndpointPrefix());

        try {
            Retrofit retrofit = getRetrofitInstance(api);

            // Fetch network interceptors to find out what kind of authentication is it using
            List<Interceptor> interceptors = ((OkHttpClient) retrofit.callFactory()).networkInterceptors();
            // There should be only one interceptor (Authentication=token)
            Assert.assertTrue(interceptors.size()==1);

            // Authentication comes in key-value pairs, check that these fields correspond to the selected authentication method
            InterceptorKeyPair interceptorKeyPair = new InterceptorKeyPair(retrofit, interceptors).invoke();
            Object keyPair1 = interceptorKeyPair.getKeyPair1();
            Object keyPair2 = interceptorKeyPair.getKeyPair2();

            Assert.assertTrue(keyPair1.toString().contains("token") || keyPair1.toString().contains("Authorization"));
            Assert.assertTrue(keyPair2.toString().contains("token") || keyPair2.toString().contains("Authorization"));
            Assert.assertFalse(keyPair1.toString().contains("Cookie") || keyPair2.toString().contains("Cookie"));

            // TODO: check url
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMesosApiBuilderWithBasicAuth() {
        String ppal = "principal";
        String secret = "secret";
        String authentication = Credentials.basic(ppal, secret);

        MesosApi api = ApiBuilder.build(ppal, secret, "http://leader.mesos:5050", MesosApi.class);
        Assert.assertFalse(api.hasEndpointPrefix());

        try {
            Retrofit retrofit = getRetrofitInstance(api);

            // Fetch network interceptors to find out what kind of authentication is it using
            List<Interceptor> interceptors = ((OkHttpClient) retrofit.callFactory()).networkInterceptors();
            // There should be only one interceptor (Authentication=Basic AafafASD==)
            Assert.assertTrue(interceptors.size()==1);
            InterceptorKeyPair interceptorKeyPair = new InterceptorKeyPair(retrofit, interceptors).invoke();
            Object keyPair1 = interceptorKeyPair.getKeyPair1();
            Object keyPair2 = interceptorKeyPair.getKeyPair2();

            Assert.assertTrue(keyPair1.toString().contains(authentication) || keyPair1.toString().contains("Authorization"));
            Assert.assertTrue(keyPair2.toString().contains(authentication) || keyPair2.toString().contains("Authorization"));
            Assert.assertFalse(keyPair1.toString().contains("Cookie") || keyPair2.toString().contains("Cookie"));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMesosApiBuilderWithoutAuth() {
        MesosApi api = ApiBuilder.build("http://leader.mesos:5050", MesosApi.class);
        Assert.assertFalse(api.hasEndpointPrefix());
        try {
            Retrofit retrofit = getRetrofitInstance(api);
            // Fetch network interceptors to find out what kind of authentication is it using
            List<Interceptor> interceptors = ((OkHttpClient) retrofit.callFactory()).networkInterceptors();
            Assert.assertTrue(interceptors.size()==0);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    private Retrofit getRetrofitInstance(MesosApi api) throws NoSuchFieldException, IllegalAccessException {
        Field mesosInterface = api.getClass().getDeclaredField("mesosInterface");
        mesosInterface.setAccessible(true);
        Proxy o = (Proxy) mesosInterface.get(api);
        // extract retrofit object via reflection proxy
        InvocationHandler invocationHandler = Proxy.getInvocationHandler(o);
        Field innerProxy = Arrays.stream(invocationHandler.getClass().getDeclaredFields())
                .filter(f -> f.getName().contains("this"))
                .findFirst().orElse(null);
        innerProxy.setAccessible(true);
        return (Retrofit) innerProxy.get(invocationHandler);
    }

    private class MockResponse extends ResponseBody {
        private String mediaType;
        private long length;
        private File body;

        public MockResponse(String mediaType, long length, File body) {
            this.mediaType = mediaType;
            this.length = length;
            this.body = body;
        }

        @Override
        public MediaType contentType() {
            return MediaType.parse(this.mediaType);
        }

        @Override
        public long contentLength() {
            return this.length;
        }

        @Override
        public BufferedSource source() {
            try {
                if (body!=null) {
                    return Okio.buffer(Okio.source(body));
                } else {
                    return null;
                }
            } catch (FileNotFoundException e) {
                return null;
            }
        }
    }

    private class InterceptorKeyPair {
        private Retrofit retrofit;
        private List<Interceptor> interceptors;
        private Object keyPair1;
        private Object keyPair2;

        public InterceptorKeyPair(Retrofit retrofit, List<Interceptor> interceptors) {
            this.retrofit = retrofit;
            this.interceptors = interceptors;
        }

        public Object getKeyPair1() {
            return keyPair1;
        }

        public Object getKeyPair2() {
            return keyPair2;
        }

        public InterceptorKeyPair invoke() throws NoSuchFieldException, IllegalAccessException {
            // Authentication comes in key-value pairs, check that these fields correspond to the selected authentication method
            Field arg$1 = ((OkHttpClient) retrofit.callFactory()).networkInterceptors().get(0).getClass().getDeclaredField("arg$1");
            Field arg$2 = ((OkHttpClient) retrofit.callFactory()).networkInterceptors().get(0).getClass().getDeclaredField("arg$2");
            arg$1.setAccessible(true);
            arg$2.setAccessible(true);

            keyPair1 = arg$1.get(interceptors.get(0));
            keyPair2 = arg$2.get(interceptors.get(0));
            return this;
        }
    }
}
