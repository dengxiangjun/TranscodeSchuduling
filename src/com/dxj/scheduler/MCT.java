package com.dxj.scheduler;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A comparison of eleven static heuristics for mapping a class
 of independent tasks onto heterogeneous distributed computing systems
 * 最小完成时间调度算法
 */
public class MCT {

    public double schedule(Job app) {
        List<Task> tasks = app.getTasks();
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                return o1.getComplexity() >= o2.getComplexity()? 1:-1;
            }
        });
        List<Node> nodes = app.getNodes();
        int delay = 20;
        for (Task task : tasks) {
            double minFt = Double.MAX_VALUE;
            Node selectedNode = null;

            for (Node node : nodes) {
                double ft = node.getFt() + task.getComplexity() / node.getCapacity() + delay;
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

        double appFt = Double.MIN_VALUE;
        for (Node node : nodes) {
            appFt = Math.max(appFt,node.getFt());
        }
        return appFt;
    }
}
