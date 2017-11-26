package com.dxj.scheduler;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class MLFT {

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
        int m = nodes.size(), k_th_max = 20;
        double delay = 20, f_average = sumComplexity / sumCapacity + delay * tasks.size() / m;
        double jobFt = Double.MAX_VALUE;
        for (int k_th = 1; k_th < k_th_max; k_th++) {
            for (Node node : nodes) {
                node.setFt(0d);
                node.setTasks(new ArrayList<>());
            }
            List<Task> newTasks = new ArrayList<>();
            double c_thr = sumComplexity / (m * k_th);//分割阈值
            for (Task task : tasks) {
                if (task.getComplexity() < c_thr) {//如果小于分割阈值
                    newTasks.add(task);
                } else {//大于分割阈值需要重新分割
                    List<Integer> subComplexitys = task.getSubComplexitys();
                    List<Task> subTasks = new ArrayList<>();

                    int availableTaskCnt = 0;
                    int lowbound = 0;
                    for (int i = 0; i < subComplexitys.size(); i++) {
                        int subComplexty = subComplexitys.get(i);
                        int nextSubComplexty;
                        if ((i + 1) == subComplexitys.size()) nextSubComplexty = task.getComplexity();
                        else nextSubComplexty = subComplexitys.get(i + 1);

                        if (((subComplexty - lowbound) >= c_thr) || (((subComplexty - lowbound) < c_thr) && ((nextSubComplexty - lowbound) > c_thr))) {
                            int complexity = subComplexty - lowbound;
                            Task availabeTask = new Task(task.getName() + "_" + availableTaskCnt++, complexity);
                            lowbound = subComplexty;
                            subTasks.add(availabeTask);
                        }
                    }
                    newTasks.addAll(subTasks);
                }
            }

            Collections.sort(newTasks, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    return o1.getComplexity() <= o2.getComplexity() ? 1 : -1;
                }
            });

            Collections.sort(nodes, new Comparator<Node>() {
                @Override
                public int compare(Node o1, Node o2) {
                    return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
                }
            });

            int n = newTasks.size();
            double f_average_k = sumComplexity / sumCapacity + delay * n / m;

            int j = 0;
            for (int i = 0; i < n; ) {
                Task task = newTasks.get(i);
                if (j < nodes.size()) {
                    Node node = nodes.get(j);
                    double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay;
                    if (ft <= f_average_k) {
                        i++;
                        List<Task> nodeTasks = node.getTasks();
                        nodeTasks.add(task);
                        node.setTasks(nodeTasks);
                        node.setFt(ft);
                    } else j++;
                } else {
                    double minFt_k = Double.MAX_VALUE;
                    Node selectedNode = null;
                    for (Node node : nodes) {
                        double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay;
                        if (ft < minFt_k) {
                            minFt_k = ft;
                            selectedNode = node;
                        }
                    }
                    List<Task> nodeTasks = selectedNode.getTasks();
                    nodeTasks.add(task);
                    selectedNode.setTasks(nodeTasks);
                    selectedNode.setFt(minFt_k);
                    i++;
                }
            }

            double k_max_ft = Double.MIN_VALUE;
            for (Node node:nodes){
                k_max_ft = Math.max(k_max_ft,node.getFt());
            }
            jobFt = Math.min(jobFt,k_max_ft);
//            double k_maxFt = Double.MIN_VALUE, k_minFt = Double.MAX_VALUE;
//            int k_maxFt_index = 0, k_minFt_index = 0;
//            for (int i = 0; i < m; i++) {
//                Node node = nodes.get(i);
//                double ft = node.getFt();
//                if (k_maxFt < ft) {
//                    k_maxFt_index = i;
//                    k_maxFt = ft;
//                }
//                if (k_minFt > ft) {
//                    k_minFt_index = i;
//                    k_minFt = ft;
//                }
//            }
//            System.out.println("最长队列与最短队列重分布前： f_average_k: " + f_average_k + "; f_average: " + f_average + "; k_maxFt: " + k_maxFt);
//            List<Task> boundTasks = new ArrayList<>();
//            Node maxFt_node = nodes.get(k_maxFt_index),minFt_node = nodes.get(k_minFt_index);
//            boundTasks.addAll(maxFt_node.getTasks());
//            boundTasks.addAll(minFt_node.getTasks());
//
//            maxFt_node.setTasks(new ArrayList<>());
//            minFt_node.setTasks(new ArrayList<>());
//
//            Collections.sort(boundTasks, new Comparator<Task>() {
//                @Override
//                public int compare(Task o1, Task o2) {
//                    return o1.getComplexity() <= o2.getComplexity() ? 1 : -1;
//                }
//            });
//
//            for (Task task : boundTasks){
//                double max_node_ft = maxFt_node.getFt() + task.getComplexity() / maxFt_node.getCapacity() + delay;
//                double min_node_ft = minFt_node.getFt() + task.getComplexity() / minFt_node.getCapacity() + delay;
//                Node selectedNode;
//                double selectedFt;
//                if (max_node_ft > min_node_ft){
//                    selectedNode = minFt_node;
//                    selectedFt = min_node_ft;
//                }else {
//                    selectedNode = maxFt_node;
//                    selectedFt = max_node_ft;
//                }
//                List<Task> nodeTasks = selectedNode.getTasks();
//                nodeTasks.add(task);
//                selectedNode.setTasks(nodeTasks);
//                selectedNode.setFt(selectedFt);
//            }
//            k_maxFt = Double.MIN_VALUE;
//            for (Node node : nodes) k_maxFt = Math.max(k_maxFt,node.getFt());
//            System.out.println("最长队列与最短队列重分布后： f_average_k: " + f_average_k + "; f_average: " + f_average + "; k_maxFt: " + k_maxFt);
//            jobFt = Math.min(k_maxFt, jobFt);
        }
        return jobFt;
    }
}
