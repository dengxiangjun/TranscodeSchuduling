package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Prediction-based and Locality-aware Task Scheduling for Parallelizing Video Transcoding over Heterogeneous MapReduce Cluster
 * PLTS算法
 */
public class PLTS {

    public double schedule(Job app) {
        List<Task> tasks = app.getTasks();
        List<Node> nodes = app.getNodes();

        int c_max = Integer.MIN_VALUE, c_min = Integer.MAX_VALUE;
        for (Task task : tasks) {
            c_max = Math.max(c_max, task.getComplexity());
            c_min = Math.min(c_min, task.getComplexity());
        }
        double curr = (double) c_max / (double) c_min;
        int delay = 20;
        List<Task> assignedTask = new ArrayList<>();//已经分配的列表
        int sigma = 5;
        int taskCount = tasks.size();
        while (taskCount>0) {
            double minFtI = Double.MAX_VALUE, maxFtI = Double.MIN_VALUE;
            Task minFtTask = null, maxFtTask = null;
            Node minFtNode = null, maxFtNode = null;
            for (Task task : tasks) {
                if (assignedTask.contains(task)) continue;
                int complexity = task.getComplexity();

                for (Node node : nodes) {
                    double ect = complexity / node.getCapacity();//期望计算时间
                    double comm = getCommnicationTime(task, node);
                    double ett = node.getFt() + ect + comm + delay;
                    if (ett < minFtI) {
                        minFtI = ett;
                        minFtTask = task;
                        minFtNode = node;
                    }
                    if (ett > maxFtI) {
                        maxFtI = ett;
                        maxFtTask = task;
                        maxFtNode = node;
                    }

                }
            }

            if (curr > sigma) {
                List<Task> nodeTasks = minFtNode.getTasks();
                nodeTasks.add(minFtTask);
                minFtNode.setTasks(nodeTasks);
                minFtNode.setFt(minFtI);
                assignedTask.add(minFtTask);
            } else {
                List<Task> nodeTasks = maxFtNode.getTasks();
                nodeTasks.add(maxFtTask);
                minFtNode.setTasks(nodeTasks);
                minFtNode.setFt(maxFtI);
                assignedTask.add(maxFtTask);
            }
            taskCount --;
        }

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt, node.getFt());
        }
        return appFt;
    }

    /**
     * 任务通信开销
     *
     * @param task 任务
     * @param node 转码节点
     * @return 通信开销
     */
    public double getCommnicationTime(Task task, Node node) {
        int nodeRackNum = node.getRack().getRackNum();
        List<Node> location = task.getLocation();
        int segmentSize = task.getSegmentSize();
        if (location.contains(node)) return 0;//同一结点
        else {
            boolean isRackLocal = false;
            for (Node localtionNode : location) {
                if (localtionNode.getRack().getRackNum() == nodeRackNum) {
                    isRackLocal = true;
                    break;
                }
            }
            if (isRackLocal) return segmentSize / 128.0;//同一机架，网速128MB/s
            else return 3 * segmentSize / 12.8;//不同机架。网速12.8MB/s
        }
    }
}
