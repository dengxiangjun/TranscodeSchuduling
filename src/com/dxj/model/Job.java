package com.dxj.model;

import java.util.List;

public class Job {
    private List<Node> nodes;//作业的可用结点
    private List<Task> tasks;//作业的任务集

    public Job(List<Node> nodes, List<Task> tasks) {
        this.nodes = nodes;
        this.tasks = tasks;
    }

    public List<Node> getNodes() {

        return nodes;
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
