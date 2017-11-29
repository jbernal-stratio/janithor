package com.stratio.mesos.api.framework;

import java.util.HashMap;

/**
 * Created by alonso on 28/11/17.
 */
public class MesosResource {
    private String name;
    private String type;
    private String role;
    private HashMap<String, Double> scalar;
    private HashMap<String, Object> ranges;
    private HashMap<String, Object> reservation;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public HashMap<String, Double> getScalar() {
        return scalar;
    }

    public void setScalar(HashMap<String, Double> scalar) {
        this.scalar = scalar;
    }

    public HashMap<String, Object> getRanges() {
        return ranges;
    }

    public void setRanges(HashMap<String, Object> ranges) {
        this.ranges = ranges;
    }

    public HashMap<String, Object> getReservation() {
        return reservation;
    }

    public void setReservation(HashMap<String, Object> reservation) {
        this.reservation = reservation;
    }
}
