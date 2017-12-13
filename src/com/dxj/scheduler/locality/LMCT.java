package com.dxj.scheduler.locality;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.util.TaskUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 最小完成时间调度算法
 */
public class LMCT {

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
        int delay = 10;
        double sumComm = 0;
        for (Task task : tasks) {
            double minFt = Double.MAX_VALUE,minFtComm = 0;
            Node selectedNode = null;

            for (Node node : nodes) {
                double comm = TaskUtil.getCommnicationTime(task, node);
                double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay + comm;
                if (ft < minFt) {
                    minFt = ft;
                    selectedNode = node;
                    minFtComm = comm;
                }
            }
            List<Task> nodeTasks = selectedNode.getTasks();
            nodeTasks.add(task);
            selectedNode.setTasks(nodeTasks);
            selectedNode.setFt(minFt);
            sumComm += minFtComm;
        }

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt,node.getFt());
        }
        app.setComm(sumComm);
        return appFt;
    }
}
