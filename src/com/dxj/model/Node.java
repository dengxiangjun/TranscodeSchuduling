package com.dxj.model;

import java.util.ArrayList;
import java.util.List;

public class Node {
    private String name;//结点名称
    private double capacity;//转码速度
    private double ft;//结点上所有子任务转码完成时间
    private Rack rack;
    private List<Task> tasks = new ArrayList<>();//分配的任务集
    private List<Task> storageTasks = new ArrayList<>();//保存的分片

    public double getFt() {
        return ft;
    }

    public Rack getRack() {
        return rack;
    }

    public void setRack(Rack rack) {
        this.rack = rack;
    }

    public void setFt(double ft) {
        this.ft = ft;
    }

    public Node(String name, double capacity, Rack rack) {
        this.name = name;
        this.capacity = capacity;
        this.rack = rack;
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

    public List<Task> getStorageTasks() {
        return storageTasks;
    }

    public void setStorageTasks(List<Task> storageTasks) {
        this.storageTasks = storageTasks;
    }

    @Override
    public String toString() {
        return "Node{" +
                "name='" + name + '\'' +
                ", capacity=" + capacity +
                ", ft=" + ft +
                ", rack=" + rack +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (Double.compare(node.capacity, capacity) != 0) return false;
        return !(name != null ? !name.equals(node.name) : node.name != null);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = name != null ? name.hashCode() : 0;
        temp = Double.doubleToLongBits(capacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
