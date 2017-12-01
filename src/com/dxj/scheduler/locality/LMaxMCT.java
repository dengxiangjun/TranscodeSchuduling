package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing Video Transcoding Using Map-Reduce-Based Cloud Computing
 * LocalityMaxMCT调度算法
 */
public class LMaxMCT {

    public double schedule(Job job) {
        List<Task> tasks = job.getTasks();
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                return o1.getComplexity() <= o2.getComplexity() ? 1 : -1;
            }
        });
        List<Node> nodes = job.getNodes();
        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
            }
        });

        double sumComplexity = 0d;
        for (Task task : tasks) {
            sumComplexity += task.getComplexity();
        }

        double sumCapacity = 0d;
        for (Node node : nodes) {
            sumCapacity += node.getCapacity();
        }
        int delay = 10, n = tasks.size(),m = nodes.size();
        double f_average = sumComplexity / sumCapacity + delay * n / m;
        int j = 0;
        for (int i = 0; i < n;) {
            Task task = tasks.get(i);
            if (j < m) {
                Node node = nodes.get(j);
                double comm = TaskUtil.getCommnicationTime(task, node);
                double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
                if (ft <= f_average) {
                    i++;
                    List<Task> nodeTasks = node.getTasks();
                    nodeTasks.add(task);
                    node.setTasks(nodeTasks);
                    node.setFt(ft);
                } else j++;
            } else {
                double minFt = Double.MAX_VALUE;
                Node selectedNode = null;
                for (Node node : nodes) {
                    double comm = TaskUtil.getCommnicationTime(task, node);
                    double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
                    if (ft < minFt) {
                        minFt = ft;
                        selectedNode = node;
                    }
                }
                List<Task> nodeTasks = selectedNode.getTasks();
                nodeTasks.add(task);
                selectedNode.setTasks(nodeTasks);
                selectedNode.setFt(minFt);
                i++;
            }
        }

        double jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            jobFt = Math.max(jobFt, node.getFt());
        }
        return jobFt;
    }
}
