package com.dxj.model;

import java.util.ArrayList;
import java.util.List;

public class Node {
    private String name;
    private double capacity;
    private double ft;
    private List<Task> tasks = new ArrayList<>();


    public double getFt() {
        return ft;
    }

    public void setFt(double ft) {
        this.ft = ft;
    }

    public Node(String name, double capacity) {
        this.name = name;
        this.capacity = capacity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public String toString() {
        return "Node{" +
                "name='" + name + '\'' +
                ", capacity=" + capacity +
                ", ft=" + ft +
                ", tasks=" + tasks +
                '}';
    }
}
