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
public class LMLFT {

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
        int m = nodes.size(), k_th_max = 40;
        double delay = 10, f_average = sumComplexity / sumCapacity + delay * tasks.size() / m;
        System.out.println("f_average" + f_average);
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
                            int segmentSize = (int) task.getSegmentSize() * complexity / task.getComplexity();
                            Task availabeTask = new Task(task.getName() + "_" + availableTaskCnt++, complexity, segmentSize, task.getLocation());
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
                    int c1= o1.getComplexity(), c2 = o2.getComplexity();
                    if (c1< c2) return 1;
                    else if (c1 > c2) return -1;
                    else return 0;
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
            System.out.println("f_average_k" + f_average_k);
            int j = 0;
            for (int i = 0; i < n; ) {
                Task task = newTasks.get(i);
                if (j < nodes.size()) {
                    Node node = nodes.get(j);
                    double comm = TaskUtil.getCommnicationTime(task, node);
                    double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
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
                        double comm = TaskUtil.getCommnicationTime(task, node);
                        double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
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
            for (Node node : nodes) {
                k_max_ft = Math.max(k_max_ft, node.getFt());
            }
            System.out.println("k_th: "+k_th+"; k_max_ft: "+k_max_ft +";jobFt: " + jobFt);
            if (k_max_ft < jobFt){
                jobFt = k_max_ft;
            }
            jobFt = Math.min(jobFt, k_max_ft);
        }
        return jobFt;
    }
}
