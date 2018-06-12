# janithor

The aim of this project is to provide an easy interface to interact with mesos teardown, resource unreserve and exhibitor clean operations on a dc/os cluster (with and without authentication)

## Using the CLI

unzip de janithor-<version>-assembly.zip and get into the folder janithor-<version>. i.e:
```
unzip janithor-0.7.0-SNAPSHOT-assembly.zip
cd janithor-0.7.0-SNAPSHOT/
```

There are several operations available

```
usage: com.stratio.mesos.Janithor
 -a <arg>               Active/inactive frameworks (default true)
 -f,--framework <arg>   Framework name
 -h                     Show help
 -o,--operation <arg>   Operation to perform (lookup, teardown, unreserve,
                        resources, exhibitor, marathon, token)
 -p,--principal <arg>   Principal
 -r,--role <arg>        Role
 -s <arg>               Secret based authentication
 -sso <arg>             Automatic sso authentication (user:pass)
 -t <arg>               Token based authentication
 -u,--url <arg>         Mesos master url
 -x <arg>               URL prefix (mesos, master)
```

The active flags allows to filter only ACTIVE frameworks. If set to false it will only look for inactive ones


Obtain token from mesos master
```
./janithor.sh -o token -u https://leader.mesos/login?firstUser=false -sso admin:1234
```

Find framework ids
```
./janithor.sh -o lookup -u http://leader.mesos:5050 -p kafka-principal -r kafka-sec-role -f kafka-sec -t <<TOKEN>>
```

List resources for an active framework
```
./janithor.sh -o resources -u http://leader.mesos:5050 -p kafka-principal -r kafka-sec-role -f kafka-sec -t <<TOKEN>>
```

Teardown an active framework
```
./janithor.sh -o teardown -u http://leader.mesos:5050 -p kafka-principal -r kafka-sec-role -f kafka-sec -t <<TOKEN>> 
```

Unreserve resources for an inactive framework
```
./janithor.sh -o unreserve -u http://leader.mesos:5050 -p kafka-principal -r kafka-sec-role -f kafka-sec -a false -t <<TOKEN>> 
```

Unreserve all resources just by role (use with caution)
```
./janithor.sh -o dracarys -u http://leader.mesos:5050 -p kafka-principal -r kafka-sec-role -t <<TOKEN>>
 
```

Remove exhibitor service entry
```
./janithor.sh -o exhibitor -u https://sso.paas.labs.stratio.com -p kafka-principal -f dcos-service-kafka-sec -t <<TOKEN>>
```
> Note that the name of the service in exhibitor is slightly different from the registered framework in some cases

Destroy marathon service
```
./janithor.sh -o marathon -u https://megadev.labs.stratio.com/marathon/ -f kafka-sec -t <<TOKEN>>
```

## Using GUI
Run the next command
```
./janithor.sh  -i ../src/main/resources/megadev.yml
```


## Supported authentication methods

Janithor supports Token based, secret based or no authentication at all. To activate each one of them

1) **No authentication.** Do **not** set flags -t nor -s
2) **Token based.** Use the "**-t**" flag followed by the DC/OS token
3) **Mesos secret based.** Use the "**-s**" flag followed by the secret

Some operations over the Mesos API can be accessed through the url *http://master-1.node.paas.labs.stratio.com:5050* . For this
kind of operations no authentication is required really.

Some other operations like exhibitor cleanup or mesos unreserve must go through *https://sso.paas.labs.stratio.com* . In this case
either the token or the secret must be specified.

From inside the cluster, the best choice is to use the leader.mesos aliases. Here you can get some examples 

```    
private static final String HTTP_MESOS_LEADER = "http://leader.mesos:5050";
private static final String HTTP_MARATHON_LEADER = "http://leader.mesos:8080";
private static final String HTTP_EXHIBITOR_ZK1 = "http://zk-1.zk:8181";
private static final String HTTPS_SSO_LOGIN = "https://leader.mesos/login?firstUser=false";
```

## The Java API

All the features offered by the CLI can be used through the Java API as described in the wiki. Here some examples

```Java
class Test {
    public static void main(String[] args){
      String url = "https://sso.paas.labs.stratio.com";
      
      MesosApi mesos = ApiBuilder.build(url, "secret", MesosApi.class);
      ExhibitorApi exhibitor = ApiBuilder.build(url, "secret", ExhibitorApi.class);
      
      boolean teardown = mesos.teardown("framework_id");
      boolean cleanup = exhibitor.cleanup("dcos-service-kafka-sec");
    }
}
```
