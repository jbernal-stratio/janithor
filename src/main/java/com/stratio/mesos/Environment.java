package com.stratio.mesos;

import java.util.HashMap;

/**
 * Created by alonso on 28/11/17.
 */
public class Environment {
    private String[] masters;
    private HashMap<String, String> exhibitor;
    private HashMap<String, String> marathon;
    private HashMap<String, String> vault;

    public String[] getMasters() {
        return masters;
    }

    public void setMasters(String[] masters) {
        this.masters = masters;
    }

    public HashMap<String, String> getExhibitor() {
        return exhibitor;
    }

    public void setExhibitor(HashMap<String, String> exhibitor) {
        this.exhibitor = exhibitor;
    }

    public HashMap<String, String> getMarathon() {
        return marathon;
    }

    public void setMarathon(HashMap<String, String> marathon) {
        this.marathon = marathon;
    }

    public HashMap<String, String> getVault() {
        return vault;
    }

    public void setVault(HashMap<String, String> vault) {
        this.vault = vault;
    }
}
