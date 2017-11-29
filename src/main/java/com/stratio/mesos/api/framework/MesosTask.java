package com.stratio.mesos.api.framework;

/**
 * Created by alonso on 27/11/17.
 */
public class MesosTask {
    private String id;
    private String name;
    private String state;
    private String slaveId;

    public MesosTask(String literal) {
        String[] split = literal.split(":");
        this.id = split[0];
        this.name = split[1];
        this.state = split[2];
        this.slaveId = split[3];
    }

    public MesosTask() {
    }

    public MesosTask(String id, String name, String state, String slaveId) {
        this.id = id;
        this.name = name;
        this.state = state;
        this.slaveId = slaveId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(String slaveId) {
        this.slaveId = slaveId;
    }

    @Override
    public String toString() {
        return "MesosTask{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", state='" + state + '\'' +
                ", slaveId='" + slaveId + '\'' +
                '}';
    }
}
