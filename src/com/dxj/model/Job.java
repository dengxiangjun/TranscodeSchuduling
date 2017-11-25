package com.dxj.model;

import java.util.List;

public class Job {
    private List<Node> nodes;
    private List<Task> tasks;

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
