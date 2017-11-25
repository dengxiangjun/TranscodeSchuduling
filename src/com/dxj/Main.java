package com.dxj;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.scheduler.MCT;
import com.dxj.scheduler.MLFT;
import com.dxj.scheduler.MaxMCT;
import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;


public class Main {

    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<>(), nodes1 = new ArrayList<>(), nodes2 = new ArrayList<>();
        List<Task> tasks = new ArrayList<>(), tasks1 = new ArrayList<>(), tasks2 = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            double capacity = Random.nextDouble(1.0, 3.0);
            Node node = new Node("node_" + i, capacity);
            nodes.add(node);

            Node node1 = new Node("node1_" + i, capacity);
            nodes1.add(node1);

            Node node2 = new Node("node2_" + i, capacity);
            nodes2.add(node2);
        }

        for (int i = 0; i < 50; i++) {
            int complexity = Random.nextInt(15, 3600);
            Task task = new Task("task_" + i, complexity);
            tasks.add(task);

            Task task1 = new Task("task1_" + i, complexity);
            tasks1.add(task1);

            Task task2 = new Task("task2_" + i, complexity);
            task2.initSubTask(1,150);
            tasks2.add(task2);
        }

        Job job = new Job(nodes, tasks);
        MCT mct = new MCT();
        double mctFt = mct.schedule(job);
        System.out.println(mctFt);


        Job job1 = new Job(nodes1, tasks1);
        MaxMCT maxMCT = new MaxMCT();
        double maxMCTFt = maxMCT.schedule(job1);
        System.out.println(maxMCTFt);

        Job job2 = new Job(nodes2, tasks2);
        MLFT mlft = new MLFT();
        double mlftFt = mlft.schedule(job2);
        System.out.println(mlftFt);
    }
}
