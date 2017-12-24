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
 * Parallelizing Video Transcoding Using Map-Reduce-Based Cloud Computing
 * LocalityMaxMCT调度算法
 */
public class RankMCT {

    public double schedule(Job job) {
        List<Task> tasks = job.getTasks();

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
        double delay = job.getDelay();
        int  n = tasks.size(), m = nodes.size();
        double f_average = sumComplexity / sumCapacity + delay * n / m;
        List<String> assignedTask = new ArrayList<>();//已经分配的列表


        for (int i = 0; i < m; ) {
            Node node = nodes.get(i);
            List<Task> remains = new ArrayList<>();
            for (Task task : tasks)
                if (!assignedTask.contains(task.getName())) remains.add(task);

            Collections.sort(remains, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    double comm1 = TaskUtil.getCommnicationTime(o1, node);
                    double comm2 = TaskUtil.getCommnicationTime(o2, node);
                    double rank1 = o1.getComplexity() / (1 + 0), rank2 = o2.getComplexity() / (1 + 0);
                    if (rank1 < rank2) return 1;
                    else if (rank1 > rank2) return -1;
                    else return 0;
                }
            });
            for (Task task : remains) {
                double comm = TaskUtil.getCommnicationTime(task, node);
                double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
                if (ft <= f_average) {
                    List<Task> nodeTasks = node.getTasks();
                    nodeTasks.add(task);
                    node.setTasks(nodeTasks);
                    node.setFt(ft);
                    assignedTask.add(task.getName());
                } else i++;
            }
        }

        List<Task> remains = new ArrayList<>();
        for (Task task : tasks)
            if (!assignedTask.contains(task.getName())) remains.add(task);

        for (Task task : remains) {
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
        }

        double jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            jobFt = Math.max(jobFt, node.getFt());
        }
        return jobFt;
    }
}
