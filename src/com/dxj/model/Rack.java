package com.dxj.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 机架
 * Created by deng on 2017/11/27.
 */
public class Rack {
    private int rackNum;//机架编号
    private List<Node> nodes = new ArrayList<>();//机架上的结点

    public Rack(int rackNum) {
        this.rackNum = rackNum;
    }

    public int getRackNum() {
        return rackNum;
    }

    public void setRackNum(int rackNum) {
        this.rackNum = rackNum;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    @Override
    public String toString() {
        return "Rack{" +
                "rackNum=" + rackNum +
                '}';
    }
}
