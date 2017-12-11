package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class HJ {

    public double schedule(Job job) {
        List<Task> tasks = job.getTasks();
        List<Node> nodes = job.getNodes();
        double sumComplexity = 0d;
        for (Task task : tasks) {
            sumComplexity += task.getComplexity();
        }
        double sumCapacity = 0d;
        for (Node node : nodes) {
            sumCapacity += node.getCapacity();
        }
        int m = nodes.size();
        double delay = 10;

        for (Task task : tasks){
            List<Node> location = task.getLocation();
            Node selectedNode = null;
            double ft = Double.MAX_VALUE;
            for (Node node : location){
                double span = task.getComplexity() / node.getCapacity() + delay;//任务只在本地执行，选取计算能力最强的机器
                if (span < ft){
                    selectedNode = node;
                    ft = span;
                }
            }

            List<Task> nodeTasks = selectedNode.getTasks();
            nodeTasks.add(task);
            selectedNode.setTasks(nodeTasks);
            selectedNode.setFt(selectedNode.getFt() + ft);
        }

        double jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            jobFt = Math.max(jobFt,node.getFt());
        }
        return jobFt;
    }
}
