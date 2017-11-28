package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

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
        int delay = 5;
        List<Integer> assignedTask = new ArrayList<>();//已经分配的列表
        int sigma = 5;
        int taskCount = tasks.size();
        while (taskCount > 0) {
            double minFtI = Double.MAX_VALUE, maxFtI = Double.MIN_VALUE;
            int c_max = Integer.MIN_VALUE, c_min = Integer.MAX_VALUE;
            Task minFtTask = null, maxFtTask = null;
            Node minFtNode = null, maxFtNode = null;

            for (Task task : tasks) {
                int taskNum = Integer.valueOf(task.getName().substring(5));
                if (assignedTask.contains(taskNum)) continue;

                c_max = Math.max(c_max, task.getComplexity());
                c_min = Math.min(c_min, task.getComplexity());
                int complexity = task.getComplexity();

                for (Node node : nodes) {
                    double ect = complexity / node.getCapacity();//期望计算时间
                    double comm = TaskUtil.getCommnicationTime(task, node);
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

            double curr = (double) c_max / (double) c_min;
//            if (curr > sigma) {
//                List<Task> nodeTasks = maxFtNode.getTasks();
//                nodeTasks.add(maxFtTask);
//                maxFtNode.setTasks(nodeTasks);
//                maxFtNode.setFt(maxFtI);
//                int maxtaskNum = Integer.valueOf(maxFtTask.getName().substring(5));
//                assignedTask.add(maxtaskNum);
//            } else {
                List<Task> nodeTasks = minFtNode.getTasks();
                nodeTasks.add(minFtTask);
                minFtNode.setTasks(nodeTasks);
                minFtNode.setFt(minFtI);
                int mintaskNum = Integer.valueOf(minFtTask.getName().substring(5));
                assignedTask.add(mintaskNum);
           // }
            taskCount--;
        }

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt, node.getFt());
        }
        return appFt;
    }
}
