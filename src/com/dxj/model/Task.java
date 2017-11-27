package com.dxj.model;

import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;

public class Task {
    private String name;//任务名称
    private int complexity;//任务复杂度
    private List<Node> location;//任务（视频分片）所在位置
    private Node executeNode;//任务执行的结点
    private double makespan;//任务的执行时间

    public int getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
    }

    private int segmentSize;//分片大小(MB)
    private List<Integer> subComplexitys = new ArrayList<>();//I帧刻度值

    public double getMakespan() {
        return makespan;
    }

    public List<Node> getLocation() {
        return location;
    }

    public void setLocation(List<Node> location) {
        this.location = location;
    }

    public void setMakespan(double makespan) {
        this.makespan = makespan;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getComplexity() {
        return complexity;
    }

    public void setComplexity(int complexity) {
        this.complexity = complexity;
    }

    public Node getExecuteNode() {
        return executeNode;
    }

    public void setExecuteNode(Node executeNode) {
        this.executeNode = executeNode;
    }

    public List<Integer> getSubComplexitys() {
        return subComplexitys;
    }

    public void setSubComplexitys(List<Integer> subComplexitys) {
        this.subComplexitys = subComplexitys;
    }

    public Task(String name, int complexity) {
        this.name = name;
        this.complexity = complexity;
    }

    public Task(String name, int complexity, List<Node> location) {
        this.name = name;
        this.complexity = complexity;
        this.location = location;
    }

    @Override
    public String toString() {
        return "Task{" +
                "name='" + name + '\'' +
                ", complexity=" + complexity +
                ", location=" + location +
                ", executeNode=" + executeNode +
                ", makespan=" + makespan +
                ", subComplexitys=" + subComplexitys +
                '}';
    }

    /**
     * 子任务初始化，把一个任务为复杂度complexity初始化为n个子任务，子任务复杂度大小是随机的
     *
     * @param low_bound   分片粒度的下界
     * @param upper_bound 分片粒度的上界
     */
    public void initSubTask(int low_bound, int upper_bound) {
        int subComplexity;
        do {
            int randomComplexity = Random.nextInt(low_bound, upper_bound);
            if (subComplexitys.size() > 0) {
                int prevComplexity = subComplexitys.get(subComplexitys.size() - 1);
                subComplexity = prevComplexity + randomComplexity;
                if (subComplexity < complexity) subComplexitys.add(subComplexity);
                else break;
            } else {
                subComplexity = randomComplexity;
                if (subComplexity < complexity) subComplexitys.add(subComplexity);
                else break;
            }
        } while (subComplexity < complexity);
    }
}
