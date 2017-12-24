package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;

import java.util.List;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class HJ1 {

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
        double delay = job.getDelay();

        for (Task task : tasks){
            List<Node> location = task.getLocation();
            Node selectedNode = null;
            double ft = Double.MAX_VALUE;
            for (Node node : location){
                double nodeFt = node.getFt() + task.getComplexity() / node.getCapacity() + delay;//任务只在本地执行，选取最早执行完的
                if (nodeFt < ft){
                    selectedNode = node;
                    ft = nodeFt;
                }
            }

            List<Task> nodeTasks = selectedNode.getTasks();
            nodeTasks.add(task);
            selectedNode.setTasks(nodeTasks);
            selectedNode.setFt(ft);
        }

        double jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            jobFt = Math.max(jobFt,node.getFt());
        }
        return jobFt;
    }
}
