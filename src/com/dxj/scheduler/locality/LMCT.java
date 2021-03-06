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
 * 最小完成时间调度算法
 */
public class LMCT  implements Scheduler {

    public double schedule(Job app) {
        List<Task> tasks = app.getTasks();
//        Collections.sort(tasks, new Comparator<Task>() {
//            @Override
//            public int compare(Task o1, Task o2) {
//                int c1 = o1.getComplexity(), c2 = o2.getComplexity();
//                if (c1 < c2) return 1;
//                else if (c1 > c2) return -1;
//                else return 0;
//            }
//        });
        List<Node> nodes = app.getNodes();
        double delay = app.getDelay();
        double sumComm = 0,sumMakespan = 0;
        for (Task task : tasks) {
            double minFt = Double.MAX_VALUE,minFtComm = 0,makespan = 0;
            Node selectedNode = null;

            for (Node node : nodes) {
                double comm = TaskUtil.getCommnicationTime(task, node);
                double predictMakespan = task.getComplexity() / node.getCapacity() + delay + comm;
                double ft = node.getFt() + predictMakespan;
                if (ft < minFt) {
                    minFt = ft;
                    selectedNode = node;
                    minFtComm = comm;
                    makespan = predictMakespan;

                }
            }
            TaskUtil.taskAssign(task, selectedNode, makespan, minFtComm, minFt);
            sumComm +=minFtComm;
            sumMakespan += makespan;
//            List<Task> nodeTasks = selectedNode.getTasks();
//            nodeTasks.add(task);
//            selectedNode.setTasks(nodeTasks);
//            selectedNode.setFt(minFt);
//            sumComm += minFtComm;
        }

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt,node.getFt());
        }
        app.setComm(sumComm);
        app.setMakespan(sumMakespan);
        return appFt;
    }
}
