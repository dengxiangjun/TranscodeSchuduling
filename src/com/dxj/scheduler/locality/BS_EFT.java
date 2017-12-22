package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.scheduler.Scheduler;
import com.dxj.util.JobUtil;
import com.dxj.util.TaskUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class BS_EFT implements Scheduler{

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

        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
            }
        });

        int m = nodes.size();
        double delay = 5;
        double finalFt = Double.MAX_VALUE, minSpan = Double.MAX_VALUE;
        int c1 = totalComplexity,bestC = 0;
        while (c1 > 0) {
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
                        else break;
                    }
                    firstRemoteComplexity = task.getComplexity() - lastLocalComplexity;
                    if (lastLocalComplexity > 0) {
                        Task lastLocalTask = new Task(task.getName() + "_1", lastLocalComplexity,
                                task.getSegmentSize() * lastLocalComplexity / task.getComplexity(), task.getLocation());
                        lastLocalTask.setSubComplexitys(subComplexity.subList(0, k));
                        localTasks.add(lastLocalTask);
                    }

                    if (firstRemoteComplexity > 0) {
                        Task firstRemoteTask = new Task(task.getName() + "_2", firstRemoteComplexity,
                                task.getSegmentSize() * firstRemoteComplexity / task.getComplexity(), task.getLocation());
                        firstRemoteTask.setSubComplexitys(subComplexity.subList(k, subComplexity.size()));
                        remoteTaks.add(firstRemoteTask);
                    }
                    i++;
                    break;
                }
            }

            for (; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                remoteTaks.add(task);
            }

            Collections.sort(localTasks, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                    if (c1 < c2) return 1;
                    else if (c1 > c2) return -1;
                    else return 0;
                }
            });

            Collections.sort(remoteTaks, new Comparator<Task>() {
                @Override
                public int compare(Task o1, Task o2) {
                    int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                    if (c1 < c2) return 1;
                    else if (c1 > c2) return -1;
                    else return 0;
                }
            });
            int sum1 = 0, sum2 = 0;
            double lolcalSpan = 0, remoteSpan = 0;
            for (Task task : localTasks) {//先分配本地任务，再分配远程任务
                sum1 += task.getComplexity();
                List<Node> location = task.getLocation();
//                Collections.sort(location, new Comparator<Node>() {
//                    @Override
//                    public int compare(Node o1, Node o2) {
//                        return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
//                    }
//                });
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
                lolcalSpan += makespan;
                TaskUtil.taskAssign(task, selectedNode, makespan, 0, ft);
            }

            for (Task task : remoteTaks) {
                sum2 += task.getComplexity();
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
                remoteSpan += makespan;
                TaskUtil.taskAssign(task, selectedNode, makespan, comm, ft);
            }

            double jobFt = Double.MIN_VALUE, sumFt = 0;
            for (Node node : nodes) {
                sumFt += node.getFt();
                jobFt = Math.max(jobFt, node.getFt());
            }
            // double roundSpan =
            //System.out.println("本轮调度结果: " + jobFt+",c1: "+c1 +"; sumFt: "+ sumFt + "  ;lolcalSpan: "+lolcalSpan + " ;remoteSpan: " + remoteSpan + " ;sumComplexity: " + (sum1 + sum2));

            finalFt = Math.min(finalFt, jobFt);
            if (sumFt < minSpan){
                bestC = c1;
                minSpan = sumFt;
            }

            JobUtil.clear(job);
            c1 -= 5;
        }

        double ft_average = totalComplexity / sumCapacity + delay * tasks.size() / m;

        System.out.println("ft_average: " + ft_average + "; minSpan: " + minSpan + " ;OptimalMakeSpan: " + (m*ft_average));
        job.setMakespan(minSpan);



        int localSumComplexity = 0;
        List<Task> localTasks = new ArrayList<>(), remoteTaks = new ArrayList<>();
        int i = 0;
        for (; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            localSumComplexity += task.getComplexity();
            if (localSumComplexity <= bestC) {
                localTasks.add(task);
            } else {
                int preComplexity = localSumComplexity - task.getComplexity();
                int overflow = bestC - preComplexity;

                List<Integer> subComplexity = task.getSubComplexitys();
                int lastLocalComplexity = 0, firstRemoteComplexity = 0, k = 0;
                for (; k < subComplexity.size(); k++) {
                    int scale = subComplexity.get(k);
                    if (overflow >= scale) lastLocalComplexity = scale;
                    else break;
                }
                firstRemoteComplexity = task.getComplexity() - lastLocalComplexity;
                if (lastLocalComplexity > 0) {
                    Task lastLocalTask = new Task(task.getName() + "_1", lastLocalComplexity,
                            task.getSegmentSize() * lastLocalComplexity / task.getComplexity(), task.getLocation());
                    lastLocalTask.setSubComplexitys(subComplexity.subList(0, k));
                    localTasks.add(lastLocalTask);
                }

                if (firstRemoteComplexity > 0) {
                    Task firstRemoteTask = new Task(task.getName() + "_2", firstRemoteComplexity,
                            task.getSegmentSize() * firstRemoteComplexity / task.getComplexity(), task.getLocation());
                    firstRemoteTask.setSubComplexitys(subComplexity.subList(k, subComplexity.size()));
                    remoteTaks.add(firstRemoteTask);
                }
                i++;
                break;
            }
        }

        for (; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            remoteTaks.add(task);
        }

        Collections.sort(localTasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                if (c1 < c2) return 1;
                else if (c1 > c2) return -1;
                else return 0;
            }
        });

        Collections.sort(remoteTaks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
                if (c1 < c2) return 1;
                else if (c1 > c2) return -1;
                else return 0;
            }
        });
        int sum1 = 0, sum2 = 0;
        double lolcalSpan = 0, remoteSpan = 0;
        for (Task task : localTasks) {
            sum1 += task.getComplexity();
            List<Node> location = task.getLocation();
//                Collections.sort(location, new Comparator<Node>() {
//                    @Override
//                    public int compare(Node o1, Node o2) {
//                        return o1.getCapacity() <= o2.getCapacity() ? 1 : -1;
//                    }
//                });
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
            lolcalSpan += makespan;
            TaskUtil.taskAssign(task, selectedNode, makespan, 0, ft);
        }

        for (Task task : remoteTaks) {
            sum2 += task.getComplexity();
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
            remoteSpan += makespan;
            TaskUtil.taskAssign(task, selectedNode, makespan, comm, ft);
        }

        double jobFt = Double.MIN_VALUE, sumFt = 0;
        for (Node node : nodes) {
            sumFt += node.getFt();
            jobFt = Math.max(jobFt, node.getFt());
        }

        return finalFt;
    }
}
