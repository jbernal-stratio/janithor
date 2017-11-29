package com.stratio.mesos.api.framework;

/**
 * Created by alonso on 27/11/17.
 */
public class MesosFramework {
    private Boolean active;
    private String id;
    private String name;
    private String role;
    private String principal;

    public MesosFramework(String literal) {
        String[] split = literal.split(":");
        this.active = Boolean.valueOf(split[0]);
        this.id = split[1];
        this.name = split[2];
        this.role = split[3];
        this.principal = split[4];
    }

    public MesosFramework() {
    }

    public MesosFramework(Boolean active, String id, String name, String role, String principal) {
        this.active = active;
        this.id = id;
        this.name = name;
        this.role = role;
        this.principal = principal;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
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

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String toString() {
        return "MesosFramework{" +
                "active=" + active +
                ", id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", role='" + role + '\'' +
                ", principal='" + principal + '\'' +
                '}';
    }
}
