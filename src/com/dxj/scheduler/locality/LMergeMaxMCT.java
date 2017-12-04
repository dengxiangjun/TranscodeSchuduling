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
 * LMergeMaxMCT调度算法
 */
public class LMergeMaxMCT {

    public double schedule(Job job) {

        List<Task> tasks = job.getTasks();

        List<Node> nodes = job.getNodes();
        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
            }
        });


//        for ()
//
        int sumComplexity = 0;
//        double sumSize = 0d;
//        for (Task task : tasks) {
//            sumComplexity += task.getComplexity();
//            sumSize += task.getSegmentSize();
//        }


        List<Integer> toalSubComplexitys = new ArrayList<>();
        for (Task task : tasks) {
            for (Integer itemSubComplexity : task.getSubComplexitys()) {
                toalSubComplexitys.add(sumComplexity + itemSubComplexity);
            }
            sumComplexity += task.getComplexity();
            toalSubComplexitys.add(sumComplexity);
        }

        double sumCapacity = 0d;
        for (Node node : nodes) {
            sumCapacity += node.getCapacity();
        }

        List<Task> newTasks = new ArrayList<>();
        int m = nodes.size();
        double delay = 5, f_avarage = sumComplexity / sumCapacity + delay;
        int lowbound = 0, availableTaskCnt = 0, i = 0;
        for (Node node : nodes) {
            int taskCnt = toalSubComplexitys.size();
            double taskComplexity = node.getCapacity() * f_avarage;
            //System.out.println("taskComplexity" + taskComplexity);
            while (i < taskCnt) {
                int currentComplexity = toalSubComplexitys.get(i);
                int nextComplexity;
                if ((i + 1) == taskCnt) nextComplexity = sumComplexity;
                else nextComplexity = toalSubComplexitys.get(i + 1);
                if ((currentComplexity - lowbound) >= taskComplexity ||
                        ((currentComplexity - lowbound) < taskComplexity && (nextComplexity - lowbound) >= taskComplexity)
                        || (i == taskCnt -1)) {
                    int complexity = currentComplexity - lowbound;
                    Task availabeTask = new Task("task_" + availableTaskCnt++, complexity);
                    newTasks.add(availabeTask);
                    lowbound = currentComplexity;
                    i++;
                    break;
                }
                i++;
            }
//            System.out.println("lowbound" + lowbound);
//            System.out.println("--------------------------------");
        }
        int b = 1;
        //Task tolalTask = new Task("integral task", sumComplexity);
        //tolalTask.setSubComplexitys(toalSubComplexitys);
//        double jobFt = Double.MAX_VALUE;
//        int m = nodes.size(), k_th_max = 40;
//        double delay = 5, f_avarage = sumComplexity / sumCapacity +
//                sumSize / (3 * TaskUtil.rackLocalSpeed) + 2 * sumSize / (3 * TaskUtil.rackCrossSpeed)
//                + delay;
//        for (int k_th = 1; k_th < k_th_max; k_th++) {
//            for (Node node : nodes) {
//                node.setFt(0d);
//                node.setTasks(new ArrayList<>());
//            }
//            List<Task> newTasks = new ArrayList<>();
//            double c_thr = sumComplexity / (m * k_th);//分割阈值
//            if (tolalTask.getComplexity() < c_thr) {//如果小于分割阈值
//                newTasks.add(tolalTask);
//            } else {//大于分割阈值需要重新分割
//                List<Integer> subComplexitys = tolalTask.getSubComplexitys();
//                List<Task> subTasks = new ArrayList<>();
//
//                int availableTaskCnt = 0;

//                int lowbound = 0;
//                for (int i = 0; i < subComplexitys.size(); i++) {
//                    int subComplexty = subComplexitys.get(i);
//                    int nextSubComplexty;
//                    if ((i + 1) == subComplexitys.size()) nextSubComplexty = tolalTask.getComplexity();
//                    else nextSubComplexty = subComplexitys.get(i + 1);
//
//                    if (((subComplexty - lowbound) >= c_thr) || (((subComplexty - lowbound) < c_thr) && ((nextSubComplexty - lowbound) > c_thr))) {
//                        int complexity = subComplexty - lowbound;
//                        Task availabeTask = new Task(tolalTask.getName() + "_" + availableTaskCnt++, complexity);
//                        lowbound = subComplexty;
//                        subTasks.add(availabeTask);
//                    }
//                }
//                newTasks.addAll(subTasks);
//            }
//
//            Collections.sort(newTasks, new Comparator<Task>() {
//                @Override
//                public int compare(Task o1, Task o2) {
//                    return o1.getComplexity() <= o2.getComplexity() ? 1 : -1;
//                }
//            });
//
//            Collections.sort(nodes, new Comparator<Node>() {
//                @Override
//                public int compare(Node o1, Node o2) {
//                    return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
//                }
//            });
//
//            int n = newTasks.size();
//            double f_average_k = sumComplexity / sumCapacity + delay * n / m;
//
//            int j = 0;
//            for (int i = 0; i < n; ) {
//                Task task = newTasks.get(i);
//                if (j < nodes.size()) {
//                    Node node = nodes.get(j);
//                    double comm = TaskUtil.getCommnicationTime(task, node);
//                    double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
//                    if (ft <= f_average_k) {
//                        i++;
//                        List<Task> nodeTasks = node.getTasks();
//                        nodeTasks.add(task);
//                        node.setTasks(nodeTasks);
//                        node.setFt(ft);
//                    } else j++;
//                } else {
//                    double minFt_k = Double.MAX_VALUE;
//                    Node selectedNode = null;
//                    for (Node node : nodes) {
//                        double comm = TaskUtil.getCommnicationTime(task, node);
//                        double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
//                        if (ft < minFt_k) {
//                            minFt_k = ft;
//                            selectedNode = node;
//                        }
//                    }
//                    List<Task> nodeTasks = selectedNode.getTasks();
//                    nodeTasks.add(task);
//                    selectedNode.setTasks(nodeTasks);
//                    selectedNode.setFt(minFt_k);
//                    i++;
//                }
//            }
//
////            for (Node node : nodes) {
////                List<Task> nodeTasks = node.getTasks();
////                Collections.sort(nodeTasks, new Comparator<Task>() {
////                    @Override
////                    public int compare(Task t1, Task t2) {
////                        int num1 = Integer.valueOf(t1.getName().substring(6)), num2 = Integer.valueOf(t2.getName().substring(6));
////                        return num1 >= num2 ? 1 : -1;
////                    }
////                });
////
////                for (int i = 0; i < nodeTasks.size() - 1; i++) {
////                    Task t1 = nodeTasks.get(i), t2 = nodeTasks.get(i + 1);
////                    int num1 = Integer.valueOf(t1.getName().substring(6)), num2 = Integer.valueOf(t2.getName().substring(6));
////                    if (num1 + 1 == num2) {
////                        node.setFt(node.getFt() - delay);
////                    }
////                }
////            }
//
//            double k_max_ft = Double.MIN_VALUE;
//            for (Node node : nodes) {
//                k_max_ft = Math.max(k_max_ft, node.getFt());
//            }
//            jobFt = Math.min(jobFt, k_max_ft);
//        }
//        return jobFt;
        return 0;
    }
}
