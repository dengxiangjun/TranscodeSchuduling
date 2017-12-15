package com.dxj.model;

import java.util.List;

public class Job {
    private List<Node> nodes;//作业的可用结点
    private List<Task> tasks;//作业的任务集
    private List<Rack> racks;
    private double comm;//通信开销
    private double makespan;

    public double getMakespan() {
        return makespan;
    }

    public void setMakespan(double makespan) {
        this.makespan = makespan;
    }

    public double getComm() {
        return comm;
    }

    public void setComm(double comm) {
        this.comm = comm;
    }

    public Job(List<Node> nodes, List<Task> tasks,List<Rack> racks) {
        this.nodes = nodes;
        this.tasks = tasks;
        this.racks = racks;
    }

    public Job(List<Node> nodes, List<Task> tasks) {
        this.nodes = nodes;
        this.tasks = tasks;
    }

    public List<Node> getNodes() {

        return nodes;
    }

    public List<Rack> getRacks() {
        return racks;
    }

    public void setRacks(List<Rack> racks) {
        this.racks = racks;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }
}
