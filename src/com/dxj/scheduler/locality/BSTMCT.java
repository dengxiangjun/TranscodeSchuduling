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
public class BSTMCT {

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
        double delay = 10;
        double jobFt = Double.MAX_VALUE;
        for (int k_th = 1; k_th < k_th_max; k_th++) {

            for (Node node : nodes) {
                node.setFt(0d);
                node.setTasks(new ArrayList<>());
            }
            List<Task> newTasks = new ArrayList<>();
            double c_thr = sumComplexity / (m * k_th);//分割阈值
            int availableTaskCnt = 0;
            for (Task task : tasks) {
                if (task.getComplexity() < c_thr) {//如果小于分割阈值
                    Task availabeTask = new Task(availableTaskCnt++ + "", task.getComplexity(), task.getSegmentSize(), task.getLocation());
                    newTasks.add(availabeTask);
                } else {//大于分割阈值需要重新分割
                    List<Integer> subComplexitys = task.getSubComplexitys();
                    List<Task> subTasks = new ArrayList<>();


                    int lowbound = 0;
                    for (int i = 0; i < subComplexitys.size(); i++) {
                        int subComplexty = subComplexitys.get(i);
                        int nextSubComplexty;
                        if ((i + 1) == subComplexitys.size()) nextSubComplexty = task.getComplexity();
                        else nextSubComplexty = subComplexitys.get(i + 1);

                        if (((subComplexty - lowbound) >= c_thr) || (((subComplexty - lowbound) < c_thr) && ((nextSubComplexty - lowbound) > c_thr))) {
                            int complexity = subComplexty - lowbound;
                            int segmentSize = (int) task.getSegmentSize() * complexity / task.getComplexity();
                            Task availabeTask = new Task(availableTaskCnt++ + "", complexity, segmentSize, task.getLocation());
                            lowbound = subComplexty;
                            subTasks.add(availabeTask);
                        }
                    }
                    newTasks.addAll(subTasks);
                }
            }

            double f_average = sumComplexity / sumCapacity + delay * newTasks.size() / m;
            Collections.sort(newTasks, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                    if (c1 < c2) return 1;
                    else if (c1 > c2) return -1;
                    else return 0;
                }
            });

            for (Task task : newTasks) {
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

            double k_max_ft = Double.MIN_VALUE;
            for (Node node : nodes) {
                k_max_ft = Math.max(k_max_ft, node.getFt());
            }

            if (k_max_ft < jobFt) {
                jobFt = k_max_ft;
            }
            jobFt = Math.min(jobFt, k_max_ft);
        }
        return jobFt;
    }
}
