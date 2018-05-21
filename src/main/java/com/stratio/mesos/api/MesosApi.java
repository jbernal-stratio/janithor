/*
 * Copyright (c) 2017. Stratio Big Data Inc., Sucursal en España. All rights reserved.
 *
 * This software – including all its source code – contains proprietary information of Stratio Big Data Inc., Sucursal
 * en España and may not be revealed, sold, transferred, modified, distributed or otherwise made available, licensed
 * or sublicensed to third parties; nor reverse engineered, disassembled or decompiled, without express written
 * authorization from Stratio Big Data Inc., Sucursal en España.
 */
package com.stratio.mesos.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.mesos.api.framework.MesosFramework;
import com.stratio.mesos.api.framework.MesosTask;
import com.stratio.mesos.http.HTTPUtils;
import com.stratio.mesos.http.MesosInterface;
import net.thisptr.jackson.jq.JsonQuery;
import okhttp3.ResponseBody;
import org.apache.log4j.Logger;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by alonso on 20/06/17.
 */
public class MesosApi {
    final static Logger LOG = Logger.getLogger(MesosApi.class);

    private ObjectMapper MAPPER = new ObjectMapper();
    private String endpointsPrefix = EndpointPrefix.EMPTY.toString();
    private MesosInterface mesosInterface;

    public enum EndpointPrefix {
        MASTER, MESOS, EMPTY;

        @Override
        public String toString() {
            switch (this) {
                case MASTER: return "master";
                case MESOS: return "mesos";
                case EMPTY: return "";
                default: throw new IllegalArgumentException();
            }
        }
    }

    public MesosApi() {
    }

    public MesosApi(String mesosMasterUrl) {
        this.mesosInterface = HTTPUtils.buildBasicInterface(mesosMasterUrl, MesosInterface.class);
    }

    public MesosApi(String accessToken, String mesosMasterUrl) {
        this.mesosInterface = HTTPUtils.buildTokenBasedInterface(accessToken, mesosMasterUrl, MesosInterface.class);
    }

    public MesosApi(String principal, String secret, String mesosMasterUrl) {
        this.mesosInterface = HTTPUtils.buildSecretBasedInterface(principal, secret, mesosMasterUrl, MesosInterface.class);
    }

    public void setEndpointsPrefix(EndpointPrefix endpointsPrefix) {
        this.endpointsPrefix = endpointsPrefix.toString();
    }

    public String getEndpointsPrefix() {
        return endpointsPrefix;
    }

    public boolean hasEndpointPrefix() { return !endpointsPrefix.isEmpty(); }

    /**
     * Finds a list of resources for a specific role inside a mesos slave
     * @param role mesos role
     * @param slaveId mesos slave id
     * @return List of JSON resources
     */
    public String[] findResourcesFor(String role, String slaveId) {
        Call<ResponseBody> mesosCall;

        try {
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.findResources();
            else mesosCall = mesosInterface.findResources(getEndpointsPrefix());

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findResourcesFor " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q;
                if (slaveId!=null) {
                    q = JsonQuery.compile(".slaves[]|select(.id==\""+slaveId+"\")|.reserved_resources_full.\""+role+"\"[]?");
                } else {
                    q = JsonQuery.compile(".slaves[]|.reserved_resources_full.\""+role+"\"[]?");
                }

                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> resources = q.apply(in);

                return resources.stream()
                        .map(resource->resource.toString())
                        .toArray(String[]::new);
            } else {
                LOG.info("Error to try fetch resources for cluster " + response.code() + " and message: " + response.errorBody());
                return null;
            }

        } catch (IOException e) {
            LOG.info("findResourcesFor failure with message " + e.getMessage());
            return null;
        }
    }

    /**
     * Unreserves a specific resource for a given slaveId.
     * The JSON must be exactly as mesos expects, otherwise it won't be accepted yielding 409 - Conflict
     * @param slaveId mesos slave id
     * @param resourceJson exact resource JSON
     * @return mesos http return code
     */
    public int unreserveResourceFor(String slaveId, String resourceJson) {
        int code = -1;
        if (resourceJson==null || slaveId==null || resourceJson.isEmpty() || slaveId.isEmpty()) {
            LOG.error("Parameters 'slaveId' and 'resourceJson' cannot be empty or null");
            return code;
        }

        Call<ResponseBody> mesosCall;

        try {
            // non-disk resources
            if (!resourceJson.toLowerCase().replace(" ", "").contains("\"disk\":{\"persistence\"")) {

                if (!hasEndpointPrefix()) mesosCall = mesosInterface.unreserve(slaveId, "[" + resourceJson + "]");
                else mesosCall = mesosInterface.unreserve(getEndpointsPrefix(), slaveId, "[" + resourceJson + "]");

                Response<ResponseBody> execute = mesosCall.clone().execute();
                code = execute.code();
                LOG.info("Unreserve standard resource returned " + code);
            } else {
                code = unreserveVolumesFor(slaveId, resourceJson);
                LOG.info("Unregister volume resource returned " + code);
            }

            System.out.println(code);
            return code;
        } catch (Exception e) {
            LOG.info("unreserveResourceFor failure with message " + e.getMessage());
            return -1;
        }
    }

    /**
     * Unreserves disk volumes from the specified slaveId
     * The JSON must be exactly as mesos expects, otherwise it won't be accepted yielding 409 - Conflict
     * @param slaveId mesos slave id
     * @param resourceJson disk resource json
     * @return mesos http return code
     */
    public int unreserveVolumesFor(String slaveId, String resourceJson) {
        if (resourceJson==null || slaveId==null || resourceJson.isEmpty() || slaveId.isEmpty()) {
            LOG.error("Parameters 'slaveId' and 'resourceJson' cannot be empty or null");
            return -1;
        }

        Call<ResponseBody> mesosCall;
        Response<ResponseBody> response;

        try {
            // destroy the volume
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.destroyVolumes(slaveId, "[" + resourceJson + "]");
            else mesosCall = mesosInterface.destroyVolumes(getEndpointsPrefix(), slaveId, "[" + resourceJson + "]");
            response = mesosCall.clone().execute();
            LOG.info("unreserveVolumesFor " + response.message());
            if (response.code() == HTTPUtils.UNRESERVE_OK_CODE) {
                // remove "disk" from JSON before unregistering resource
                HashMap<String,Object> result = MAPPER.readValue(resourceJson, HashMap.class);
                result.remove("disk");

                // unreserve the resource
                mesosCall = mesosInterface.unreserve(slaveId, "[" + MAPPER.writeValueAsString(result) + "]");
                response = mesosCall.clone().execute();
            } else {
                LOG.error("Unable to destroy volume, resource ");
            }
            return response.code();
        } catch (IOException e) {
            LOG.info("unreserveVolumesFor failure with message " + e.getMessage());
            return -1;
        }
    }

    /**
     * Performs a teardown of the specified frameworkId. A teardown <b>does not</b> imply cleaning zookeeper configuration
     * @param frameworkId
     * @return
     */
    public boolean teardown(String frameworkId) {
        if (frameworkId==null || frameworkId.isEmpty()) {
            LOG.error("Parameter 'frameworkId' cannot be null or empty");
            return false;
        }

        Call<ResponseBody> mesosCall;
        try {
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.teardown(frameworkId);
            else mesosCall = mesosInterface.teardown(getEndpointsPrefix(), frameworkId);
            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("teardown " + response.message());
            return (response.code() == HTTPUtils.HTTP_OK_CODE);
        } catch (IOException e) {
            LOG.info("teardown failure with message " + e.getMessage());
            return false;
        }
    }

    /**
     * Finds any frameworkId that matches the given serviceName, role and principal.
     * By default, it will only look for active frameworks
     * @param serviceName mesos service name
     * @param role mesos role
     * @param principal mesos principal
     * @return An optional list of framework ids
     */
    public Optional<String[]> findFrameworkId(String serviceName, String role, String principal) {
        return findFrameworkId(serviceName, role, principal, true);
    }

    /**
     * Finds any frameworkId that matches the given serviceName, role, principal and activation status
     * @param serviceName mesos service name
     * @param role mesos role
     * @param principal mesos principal
     * @param active filter by active/inactive frameworks
     * @return An optional list of framework ids
     */
    public Optional<String[]> findFrameworkId(String serviceName, String role, String principal, boolean active) {
        Call<ResponseBody> mesosCall;
        Optional<String[]> frameworkId = Optional.empty();

        try {
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.findFrameworks();
            else mesosCall = mesosInterface.findFrameworks(getEndpointsPrefix());

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findFrameworkId " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                String includeInactives = active?" and .active==true":" and .active==false";
                JsonQuery q = JsonQuery.compile(".frameworks[]|select(.name == \"" + serviceName + "\" and .role == \"" + role + "\" and .principal == \"" + principal + "\""+includeInactives+").id");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> json = q.apply(in);

                // might be a completed framework
                if (json.isEmpty()) {
                    q = JsonQuery.compile(".completed_frameworks[]|select(.name == \"" + serviceName + "\" and .role == \"" + role + "\" and .principal == \"" + principal + "\").id");
                    json = q.apply(in);
                }

                if (json.size()>0) {
                    frameworkId = Optional.of(json.stream()
                            .map(fwId -> fwId.toString().replace("\"", ""))
                            .toArray(String[]::new)
                    );
                } else if (json.size()==0) {
                    LOG.error("No frameworks found for (" + serviceName + "," + role + "," + principal + ")");
                } else {
                    LOG.error("Several frameworks found for the same (" + serviceName + "," + role + "," + principal + ")");
                }
            } else {
                LOG.info("Error finding framework ("+serviceName+","+role+","+principal+"): " + response
                        .code() + " and message: " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findFrameworkId failure with message " + e.getMessage());
        } finally {
            return frameworkId;
        }
    }

    /**
     * Returns the list of mesos slaves where the specified framework is found
     * @param frameworkId framework to locate inside the mesos slaves
     * @return Optional list of slaveIds
     */
    public Optional<String[]> findSlavesForFramework(String frameworkId) {
        Call<ResponseBody> mesosCall;
        Optional<String[]> slaveIds = Optional.empty();

        try {
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.findFrameworks();
            else mesosCall = mesosInterface.findFrameworks(getEndpointsPrefix());

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findSlavesForFramework " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".frameworks[]|select(.id==\""+frameworkId+"\").tasks[].slave_id");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> slaves = q.apply(in);

                // might be a completed framework
                if (slaves.isEmpty()) {
                    q = JsonQuery.compile(".completed_frameworks[]|select(.id==\""+frameworkId+"\").completed_tasks[].slave_id");
                    slaves = q.apply(in);
                }

                slaveIds = Optional.of(slaves.stream()
                        .map(slave->slave.toString().replace("\"", ""))
                        .toArray(String[]::new));
            } else {
                LOG.info("Error finding slaves for framework ("+frameworkId+") returned "+response.code()+" - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findSlavesForFramework failure with message " + e.getMessage());
        } finally {
            return slaveIds;
        }
    }

    /**
     * Returns all available slaves ids
     * @return Optional list of slaveIds
     */
    public Optional<String[]> findAllSlaves() {
        Call<ResponseBody> mesosCall;
        Optional<String[]> slaveIds = Optional.empty();

        try {
            if (!hasEndpointPrefix()) mesosCall = mesosInterface.findFrameworks();
            else mesosCall = mesosInterface.findFrameworks(getEndpointsPrefix());

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findAllSlaves " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".frameworks[].tasks[].slave_id");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> slaves = q.apply(in);
                slaveIds = Optional.of(slaves.stream()
                        .map(slave->slave.toString().replace("\"", ""))
                        .filter(distinctByKey(p -> p.toString()))
                        .toArray(String[]::new));
            } else {
                LOG.info("Error finding all slaves. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findSlavesForFramework failure with message " + e.getMessage());
        } finally {
            return slaveIds;
        }
    }

    /**
     * Finds the leader of the cluster
     * @return mesos master "host:port"
     */
    public Optional<String> findMesosMaster() {
        Call<ResponseBody> mesosCall;
        Optional<String> mesosMaster = Optional.empty();

        try {
            mesosCall = mesosInterface.state();

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findMesosMaster " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".leader_info | \"\\(.hostname):\\(.port)\"");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> hostNode = q.apply(in);
                mesosMaster = hostNode.stream()
                        .map(slave->slave.toString().replace("\"", ""))
                        .findFirst();
            } else {
                LOG.info("Error finding mesos master. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findMesosMaster failure with message " + e.getMessage());
        } finally {
            return mesosMaster;
        }
    }

    /**
     * Returns a list of frameworks by activation status
     * @param active find active or inactive framework
     * @return list of frameworks found
     */
    public Optional<List<MesosFramework>> findFrameworks(boolean active) {
        Call<ResponseBody> mesosCall;
        Optional<List<MesosFramework>> frameworks = Optional.empty();

        try {
            mesosCall = mesosInterface.state();

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findFrameworks " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".frameworks[] | select(.active=="+active+") | \"\\(.active):\\(.id):\\(.name):\\(.role):\\(.principal)\"");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> lstFrameworks = q.apply(in);
                frameworks = Optional.of(lstFrameworks.stream()
                        .map(list -> list.toString().replace("\"", ""))
                        .map(fwk -> new MesosFramework(fwk))
                        .collect(Collectors.toList()));
            } else {
                LOG.info("Error finding frameworks. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findFrameworks failure with message " + e.getMessage());
        } finally {
            return frameworks;
        }
    }

    /**
     * Returns a list of frameworks by activation status
     * @param frameworkId framework unique identifier
     * @return list of frameworks found
     */
    public Optional<MesosFramework> findFrameworkById(String frameworkId) {
        Call<ResponseBody> mesosCall;
        Optional<MesosFramework> frameworks = Optional.empty();

        try {
            mesosCall = mesosInterface.state();

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findFrameworks " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".frameworks[] | select(.id==\""+frameworkId+"\") | \"\\(.active):\\(.id):\\(.name):\\(.role):\\(.principal)\"");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> lstFrameworks = q.apply(in);
                frameworks = lstFrameworks.stream()
                        .map(list -> list.toString().replace("\"", ""))
                        .map(fwk -> new MesosFramework(fwk))
                        .findFirst();
            } else {
                LOG.info("Error finding framework. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findFramework failure with message " + e.getMessage());
        } finally {
            return frameworks;
        }
    }

    /**
     * Returns a list of frameworks by activation status
     * @param name framework name
     * @return list of frameworks found
     */
    public Optional<MesosFramework> findFrameworkByName(String name, boolean completed) {
        Call<ResponseBody> mesosCall;
        Optional<MesosFramework> frameworks = Optional.empty();

        try {
            mesosCall = mesosInterface.state();

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findFrameworks " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                if (!completed) {
                    JsonQuery q = JsonQuery.compile(".frameworks[] | select(.name==\"" + name + "\") | \"\\(.active):\\(.id):\\(.name):\\(.role):\\(.principal)\"");
                    JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                    List<JsonNode> lstFrameworks = q.apply(in);
                    frameworks = lstFrameworks.stream()
                        .map(list -> list.toString().replace("\"", ""))
                        .map(fwk -> new MesosFramework(fwk))
                        .findFirst();
                } else {
                    JsonQuery q = JsonQuery.compile(".completed_frameworks[] | select(.name==\"" + name + "\") | \"\\(.active):\\(.id):\\(.name):\\(.role):\\(.principal)\"");
                    JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                    List<JsonNode> lstFrameworks = q.apply(in);
                    frameworks = lstFrameworks.stream()
                        .map(list -> list.toString().replace("\"", ""))
                        .map(fwk -> new MesosFramework(fwk))
                        .findFirst();
                }
            } else {
                LOG.info("Error finding framework. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findFramework failure with message " + e.getMessage());
        } finally {
            return frameworks;
        }
    }

    public Optional<List<MesosTask>> findTasksFor(String frameworkId) {
        Call<ResponseBody> mesosCall;
        Optional<List<MesosTask>> tasks = Optional.empty();

        try {
            mesosCall = mesosInterface.state();

            Response<ResponseBody> response = mesosCall.clone().execute();
            LOG.info("findTasksFor " + response.message());
            if (response.code() == HTTPUtils.HTTP_OK_CODE) {
                JsonQuery q = JsonQuery.compile(".frameworks[] | select(.id==\""+frameworkId+"\") | .tasks[] | \"\\(.id):\\(.name):\\(.state):\\(.slave_id):\\(.framework_id)\"");
                JsonNode in = MAPPER.readTree(new String(response.body().bytes()));
                List<JsonNode> lstFrameworks = q.apply(in);
                tasks = Optional.of(lstFrameworks.stream()
                        .map(list -> list.toString().replace("\"", ""))
                        .map(fwk -> new MesosTask(fwk))
                        .collect(Collectors.toList()));
            } else {
                LOG.info("Error finding tasks. Returned " + response.code() + " - " + response.errorBody());
            }
        } catch (Exception e) {
            LOG.info("findTasksFor failure with message " + e.getMessage());
        } finally {
            return tasks;
        }
    }

    private static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor)
    {
        Map<Object, Boolean> map = new ConcurrentHashMap<>();
        return t -> map.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    public void setMesosInterface(MesosInterface mesosInterface) {
        this.mesosInterface = mesosInterface;
    }
}