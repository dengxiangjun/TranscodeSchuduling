package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

import java.util.*;

/**
 * Parallelizing video transcoding with load balancing on cloud computing
 */
public class HJ3 {

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

        double jobFt = Double.MIN_VALUE, sumFt = 0;
        for (Node node : nodes) {
            sumFt += node.getFt();
            jobFt = Math.max(jobFt, node.getFt());
        }
        double ft_average = sumComplexity / sumCapacity + delay * tasks.size() / m;
        System.out.println("ft_average: " + ft_average);
        Collections.sort(nodes, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return o1.getFt() >= o2.getFt() ? 1 : -1;
            }
        });

        Map<Node, Integer> node$unBanlanceComplexity = new HashMap<>();
        for (Node node : nodes) {
            double time = ft_average - node.getFt();
            int unBanlanceComplexity = (int) (time * node.getCapacity());
            node$unBanlanceComplexity.put(node, unBanlanceComplexity);
        }

        for (int i = 0; i < m; i++) {
            Node node = nodes.get(i);
            if (node.getFt() > ft_average) break;
            List<Task> executeTasks = node.getTasks();
            List<Task> storageTasks = node.getStorageTasks();//存储的任务
            List<Task> executeInOtherNodeTasks = new ArrayList<>();

            for (Task storage : storageTasks) {
                boolean flag = false;
                for (Task execute : executeTasks) {
                    if (storage.getName().equals(execute.getName())) flag = true;
                }
                if (!flag && storage.getExecuteNode() != null) executeInOtherNodeTasks.add(storage);
            }

            int loss = node$unBanlanceComplexity.get(node) - (int) (delay * node.getCapacity());//亏，不够的复杂度
            for (Task executeInOtherNodeTask : executeInOtherNodeTasks) {
                Node executeNode = executeInOtherNodeTask.getExecuteNode();
                if (executeNode.getFt() > ft_average) {
                    int profit = -node$unBanlanceComplexity.get(executeNode);//盈，溢出的复杂度
                    int pullComplexity = 0;
                    if (loss > profit) {//亏大于盈，把整个盈拿过来
                        List<Integer> subComplexity = executeInOtherNodeTask.getSubComplexitys();

                        for (int scale : subComplexity) {
                            if (scale <= profit) pullComplexity = scale;
                            else break;
                        }
                        if (profit > executeInOtherNodeTask.getComplexity())
                            pullComplexity = executeInOtherNodeTask.getComplexity();//整个任务都拿过来
                        if (pullComplexity == 0) pullComplexity = subComplexity.get(0);

                        List<Task> assignedTasks = executeNode.getTasks();
                        assignedTasks.remove(executeInOtherNodeTask);
                        executeNode.setTasks(assignedTasks);
                        executeNode.setFt(executeNode.getFt() - executeInOtherNodeTask.getMakespan());
                        executeInOtherNodeTask.setMakespan(0);
                        executeInOtherNodeTask.setExecuteNode(null);

                        int remainComplexity = executeInOtherNodeTask.getComplexity() - pullComplexity;
                        if (remainComplexity > 0) {
                            Task remainTask = new Task(executeInOtherNodeTask.getName() + "_" + remainComplexity, remainComplexity);
                            double makespan = remainComplexity / executeNode.getCapacity() + delay;
                            double ft = executeNode.getFt() + makespan;
                            TaskUtil.taskAssign(remainTask, executeNode, makespan, 0, ft);
                        }

                        Task pullTask = new Task(executeInOtherNodeTask.getName() + "_" + pullComplexity, pullComplexity);

                        double makespan = pullComplexity / node.getCapacity() + delay;
                        double pullNodeFt = node.getFt() + makespan;
                        TaskUtil.taskAssign(pullTask, node, makespan, 0, pullNodeFt);
                    }
                }
            }

//            for (Task task : storageTasks) {
//                Node executeNode = task.getExecuteNode();//存储的任务执行的节点
//                if (!executeNode.getName().equals(node.getName())
//                        && executeNode.getFt() > ft_average) {//存储了该分片但在同一机架的另一个机器上执行
//                    double availableExecTime = ft_average - node.getFt() - delay;
//                    int availableComplexity = (int) (availableExecTime * node.getCapacity()), maxComplexity = Integer.MIN_VALUE;
//                    Task selectTask = null, maxComplexityTask = null;
//                    List<Task> tasks2 = executeNode.getTasks();
//                    for (Task task2 : tasks2) {
//                        if (availableComplexity > task2.getComplexity()) {
//                            selectTask = task2;
//                        }
//                        if (maxComplexity < task2.getComplexity()) {
//                            maxComplexity = task2.getComplexity();
//                            maxComplexityTask = task2;
//                        }
//                    }
//                    if (selectTask == null) {//可拉去的复杂度比没有可用的,则选取最大的分片切分出一块来
//
//                    } else {//如果存在可拉去的分片，则填充到node（当前打不到平均时间的）
//                        double ft = node.getFt() + selectTask.getComplexity() / node.getCapacity() + delay;
//                        TaskUtil.taskAssign(task, node, ft);
//                    }
//                }
////                List<Node> location = task.getLocation();
////                for (Node storeNode : location){
////                    if (node.getName().equals(storeNode.getName()) || storeNode.getFt() <= ft_average) continue;
////                    List<Task> tasks1 = storeNode.getTasks();
////
////                    //if (storeNode)
////                }
//            }
//            Rack rack = node.getRack();
//            List<Node> rackNodes = rack.getNodes();
//            for (Node rackNode : rackNodes) {
//                if (rackNode.getName().equals(node.getName()) || rackNode.getFt() <= ft_average) continue;
//                else {
//
//                }
//            }
        }
        jobFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            sumFt += node.getFt();
            jobFt = Math.max(jobFt, node.getFt());
        }
        return jobFt;
    }
}
