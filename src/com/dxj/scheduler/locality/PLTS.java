package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

import java.util.*;

/**
 * Prediction-based and Locality-aware Task Scheduling for Parallelizing Video Transcoding over Heterogeneous MapReduce Cluster
 * PLTS算法
 */
public class PLTS {

    public double schedule(Job app) {
        List<Task> tasks = app.getTasks();
        List<Node> nodes = app.getNodes();
        int delay = 10;
        List<Integer> assignedTask = new ArrayList<>();//已经分配的列表
        int sigma = 12;
        int taskCount = tasks.size(),sumSpan = 0;
        while (taskCount > 0) {
            int c_max = Integer.MIN_VALUE, c_min = Integer.MAX_VALUE;
            Map<Task, Double> taskMinFt = new HashMap<>();
            Map<Task, Node> taskNodeMap = new HashMap<>();
            double curr = 0;
            for (Task task : tasks) {
                int taskNum = Integer.valueOf(task.getName().substring(5));
                if (assignedTask.contains(taskNum)) continue;
                c_max = Math.max(c_max, task.getComplexity());
                c_min = Math.min(c_min, task.getComplexity());
                curr = (double) c_max / (double) c_min;
                int complexity = task.getComplexity();
                double minFtI = Double.MAX_VALUE;
                Node minFtINode = null;
                for (Node node : nodes) {
                    double ect = complexity / node.getCapacity();//期望计算时间
                    double comm = TaskUtil.getCommnicationTime(task, node);
                    double ett = node.getFt() + ect + comm + delay;
                    if (ett < minFtI) {
                        minFtI = ett;
                        minFtINode = node;
                    }
                }
                taskMinFt.put(task, minFtI);
                taskNodeMap.put(task, minFtINode);
            }
            if (curr > sigma) {
                Task maxFtTask = null;
                Node maxFtNode = null;
                double maxFtI = Double.MIN_VALUE;
                for (Map.Entry<Task, Double> entry : taskMinFt.entrySet()) {
                    if (entry.getValue() > maxFtI) {
                        maxFtI = entry.getValue();
                        maxFtTask = entry.getKey();
                        maxFtNode = taskNodeMap.get(maxFtTask);
                    }
                }
                List<Task> nodeTasks = maxFtNode.getTasks();
                nodeTasks.add(maxFtTask);
                maxFtNode.setTasks(nodeTasks);
                sumSpan += (maxFtI - maxFtNode.getFt());
                maxFtNode.setFt(maxFtI);

                int maxtaskNum = Integer.valueOf(maxFtTask.getName().substring(5));
                assignedTask.add(maxtaskNum);
            } else {
                Task minFtTask = null;
                Node minFtNode = null;
                double minFtI = Double.MAX_VALUE;
                for (Map.Entry<Task, Double> entry : taskMinFt.entrySet()) {
                    if (entry.getValue() < minFtI) {
                        minFtI = entry.getValue();
                        minFtTask = entry.getKey();
                        minFtNode = taskNodeMap.get(minFtTask);
                    }
                }
                List<Task> nodeTasks = minFtNode.getTasks();
                nodeTasks.add(minFtTask);
                minFtNode.setTasks(nodeTasks);
                sumSpan += (minFtI - minFtNode.getFt());
                minFtNode.setFt(minFtI);

                int mintaskNum = Integer.valueOf(minFtTask.getName().substring(5));
                assignedTask.add(mintaskNum);
            }
            taskCount--;
        }

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt, node.getFt());
        }
        app.setMakespan(sumSpan);
        return appFt;
    }
}

