package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.util.JobUtil;
import com.dxj.util.TaskUtil;

import java.util.*;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class HJ4 {

    public double schedule(Job job) {
        List<Task> tasks = job.getTasks();
        List<Node> nodes = job.getNodes();
        int totalComplexity = 0;
        for (Task task : tasks) {
            totalComplexity += task.getComplexity();
        }
        double sumCapacity = 0d;
        for (Node node : nodes) {
            sumCapacity += node.getCapacity();
        }
        int m = nodes.size();
        double delay = job.getDelay();
        double time = Double.MAX_VALUE, finalFt = Double.MAX_VALUE;
        int c1 = totalComplexity, c2 = totalComplexity - c1;
        int round = totalComplexity / 2;
        int j = 0;
//        Collections.sort(tasks, new Comparator<Task>() {
//            @Override
//            public int compare(Task o1, Task o2) {
//                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
//                if (c1 < c2) return 1;
//                else if (c1 > c2) return -1;
//                else return 0;
//            }
//        });
        while (j++ <= round && c1 > 0) {
            int localSumComplexity = 0;
            List<Task> localTasks = new ArrayList<>(), remoteTaks = new ArrayList<>();
            int i = 0;
            for (; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                localSumComplexity += task.getComplexity();
                if (localSumComplexity <= c1) {
                    localTasks.add(task);
                } else {
                    int preComplexity = localSumComplexity - task.getComplexity();
                    int overflow = c1 - preComplexity;
                    List<Integer> subComplexity = task.getSubComplexitys();
                    int lastLocalComplexity = 0, firstRemoteComplexity = 0, k = 0;
                    for (; k < subComplexity.size(); k++) {
                        int scale = subComplexity.get(k);
                        if (overflow >= scale) lastLocalComplexity = scale;
                        else {
                            firstRemoteComplexity = task.getComplexity() - lastLocalComplexity;
                            break;
                        }
                    }

                    Task lastLocalTask = new Task(task.getName() + "_1",lastLocalComplexity,
                            task.getSegmentSize() * lastLocalComplexity/task.getComplexity(),task.getLocation());
                    lastLocalTask.setSubComplexitys(subComplexity.subList(0,k));
                    localTasks.add(lastLocalTask);

                    Task firstRemoteTask = new Task(task.getName() + "_2",firstRemoteComplexity,
                            task.getSegmentSize() * firstRemoteComplexity/task.getComplexity(),task.getLocation());
                    firstRemoteTask.setSubComplexitys(subComplexity.subList(k,subComplexity.size()));
                    remoteTaks.add(firstRemoteTask);
                }
            }

            for (; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                remoteTaks.add(task);
            }

            for (Task task : localTasks) {
                List<Node> location = task.getLocation();
                Node selectedNode = null;
                double ft = Double.MAX_VALUE;
                double makespan = 0;
                for (Node node : location) {
                    double predictMakespan = task.getComplexity() / node.getCapacity() + delay;
                    double nodeFt = node.getFt() + predictMakespan;//任务只在本地执行，选取最早执行完的
                    if (nodeFt < ft) {
                        selectedNode = node;
                        ft = nodeFt;
                        makespan = predictMakespan;
                    }
                }
                TaskUtil.taskAssign(task, selectedNode, makespan, 0, ft);
            }

            for (Task task : remoteTaks) {
                List<Node> location = task.getLocation();
                Node selectedNode = null;
                double ft = Double.MAX_VALUE;
                double makespan = 0, comm = 0;
                for (Node node : nodes) {
                    if (location.contains(node)) continue;
                    double predictComm = TaskUtil.getCommnicationTime(task, node);
                    double predictMakespan = task.getComplexity() / node.getCapacity() + delay + comm;
                    double nodeFt = node.getFt() + predictMakespan;//任务只在远程执行，选取最早执行完的
                    if (nodeFt < ft) {
                        selectedNode = node;
                        ft = nodeFt;
                        comm = predictComm;
                        makespan = predictMakespan;
                    }
                }
                TaskUtil.taskAssign(task, selectedNode, makespan, comm, ft);
            }
            double jobFt = Double.MIN_VALUE, sumFt = 0;
            for (Node node : nodes) {
                sumFt += node.getFt();
                jobFt = Math.max(jobFt, node.getFt());
            }
            //System.out.println("本轮调度结果: " + jobFt+",c1: "+c1+"; c2: "+ c2);

            c1 -= 2;
            c2 = totalComplexity - c1;
            JobUtil.clear(job);
            finalFt = Math.min(finalFt, jobFt);
        }


//        JobUtil.clear(job);
//

//        jobFt = Double.MIN_VALUE;
//        double sumComm = 0;
//        for (Task task :tasks) sumComm +=task.getComm();
//        for (Node node : nodes) {
//            sumFt += node.getFt();
//            jobFt = Math.max(jobFt, node.getFt());
//        }
//        System.out.println("任务只在远程执行算法调度结果: " + jobFt);
        double ft_average = totalComplexity / sumCapacity + delay * tasks.size() / m;
        System.out.println("ft_average: " + ft_average);
        return finalFt;
    }
}
