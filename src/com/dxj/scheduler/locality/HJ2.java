package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class HJ2 {

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
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                if (c1 < c2) return 1;
                else if (c1 > c2) return -1;
                else return 0;
            }
        });

        for (Task task : tasks) {
            List<Node> location = task.getLocation();
            Node selectedNode = null;
            double ft = Double.MAX_VALUE;
            for (Node node : location) {
                double nodeFt = node.getFt() + task.getComplexity() / node.getCapacity() + delay;//任务只在本地执行，选取最早执行完的
                if (nodeFt < ft) {
                    selectedNode = node;
                    ft = nodeFt;
                }
            }

            List<Task> nodeTasks = selectedNode.getTasks();
            nodeTasks.add(task);
            selectedNode.setTasks(nodeTasks);
            selectedNode.setFt(ft);
            task.setExecuteNode(selectedNode);
        }

        double jobFt = Double.MIN_VALUE, sumFt = 0;
        for (Node node : nodes) {
            sumFt += node.getFt();
            jobFt = Math.max(jobFt, node.getFt());
        }
        double ft_average = sumFt / m;


        for (Task task : tasks){

        }

//        List<Rack> racks = job.getRacks();
//        for (Rack rack : racks) {
//            List<Node> rackNodes = rack.getNodes();
//            for (Node node : rackNodes) {
//
//            }
//        }
        return jobFt;
    }
}
