package com.dxj.model;

import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;

public class Task {
    private String name;//任务名称
    private int complexity;//任务复杂度
    private double segmentSize;//分片大小(MB)
    private List<Node> location;//任务（视频分片）所在位置
    private Node executeNode;//任务执行的结点
    private double makespan;//任务的执行时间
    private List<Integer> subComplexitys = new ArrayList<>();//I帧帧内刻度值

    public double getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
    }

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

    public Task(String name, int complexity, double segmentSize, List<Node> location) {
        this.name = name;
        this.complexity = complexity;
        this.segmentSize = segmentSize;
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        if (complexity != task.complexity) return false;
        if (Double.compare(task.segmentSize, segmentSize) != 0) return false;
        return !(name != null ? !name.equals(task.name) : task.name != null);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = name != null ? name.hashCode() : 0;
        result = 31 * result + complexity;
        temp = Double.doubleToLongBits(segmentSize);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Task{" +
                "name='" + name + '\'' +
                ", complexity=" + complexity +
                ", segmentSize=" + segmentSize +
                ", location=" + location +
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
