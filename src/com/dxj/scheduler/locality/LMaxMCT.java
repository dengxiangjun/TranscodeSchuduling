package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.scheduler.Scheduler;
import com.dxj.util.TaskUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing Video Transcoding Using Map-Reduce-Based Cloud Computing
 * LocalityMaxMCT调度算法
 */
public class LMaxMCT  implements Scheduler {

    public double schedule(Job job) {
        List<Task> tasks = job.getTasks();
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                if (c1 < c2) return 1;
                else if (c1 > c2) return -1;
                else return 0;
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
        int delay = 5, n = tasks.size(),m = nodes.size();
        double f_average = sumComplexity / sumCapacity + delay * n / m,sumMakespan = 0;

        int j = 0;
        for (int i = 0; i < n;) {
            Task task = tasks.get(i);
            if (j < m) {
                Node node = nodes.get(j);
                double comm = TaskUtil.getCommnicationTime(task, node);
                double predictMakespan = task.getComplexity() / node.getCapacity() + delay + comm;
                double ft = node.getFt() + predictMakespan;
                if (ft <= f_average) {
                    i++;
                    TaskUtil.taskAssign(task, node, predictMakespan, comm, ft);
                    sumMakespan +=predictMakespan;
//                    List<Task> nodeTasks = node.getTasks();
//                    nodeTasks.add(task);
//                    node.setTasks(nodeTasks);
//                    node.setFt(ft);
                } else j++;
            } else {
                double minFt = Double.MAX_VALUE,makespan = 0,minFtComm = 0;
                Node selectedNode = null;
                for (Node node : nodes) {
                    double comm = TaskUtil.getCommnicationTime(task, node);
                    double predictMakespan = task.getComplexity() / node.getCapacity() + delay + comm;
                    double ft = node.getFt() + predictMakespan;
                    if (ft < minFt) {
                        minFt = ft;
                        selectedNode = node;
                        makespan = predictMakespan;
                        minFtComm = comm;
                    }
                }
                TaskUtil.taskAssign(task, selectedNode, makespan, minFtComm, minFt);
                sumMakespan +=makespan;
//                List<Task> nodeTasks = selectedNode.getTasks();
//                nodeTasks.add(task);
//                selectedNode.setTasks(nodeTasks);
//                selectedNode.setFt(minFt);
                i++;
            }
        }

        double jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            jobFt = Math.max(jobFt, node.getFt());
        }
        job.setMakespan(sumMakespan);
        return jobFt;
    }
}
