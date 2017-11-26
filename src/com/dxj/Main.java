package com.dxj;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.scheduler.*;
import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;


public class Main {

    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<>(), nodes1 = new ArrayList<>(), nodes2 = new ArrayList<>(), nodes3 = new ArrayList<>(), nodes4 = new ArrayList<>();
        List<Task> tasks = new ArrayList<>(), tasks1 = new ArrayList<>(), tasks2 = new ArrayList<>(), tasks3 = new ArrayList<>(),tasks4 = new ArrayList<>();

        for (int i = 0; i < 25; i++) {
            double capacity = Random.nextDouble(1.0, 3.0);
            Node node = new Node("node_" + i, capacity);
            nodes.add(node);

            Node node1 = new Node("node1_" + i, capacity);
            nodes1.add(node1);

            Node node2 = new Node("node2_" + i, capacity);
            nodes2.add(node2);

            Node node3 = new Node("node3_" + i, capacity);
            nodes3.add(node3);

            Node node4 = new Node("node4_" + i, capacity);
            nodes4.add(node4);
        }

        for (int i = 0; i < 800; i++) {
            int complexity = Random.nextInt(15, 3600);
            Task task = new Task("task_" + i, complexity);
            tasks.add(task);

            Task task1 = new Task("task1_" + i, complexity);
            tasks1.add(task1);

            Task task2 = new Task("task2_" + i, complexity);
            task2.initSubTask(1, 150);
            tasks2.add(task2);

            Task task3 = new Task("task3_" + i, complexity);
            tasks3.add(task3);

            Task task4 = new Task("task4_" + i, complexity);
            tasks4.add(task4);
        }

        Job job = new Job(nodes, tasks);
        MCT mct = new MCT();
        double mctFt = mct.schedule(job);
        System.out.println("MCT算法调度结果" + mctFt);


        Job job1 = new Job(nodes1, tasks1);
        MaxMCT maxMCT = new MaxMCT();
        double maxMCTFt = maxMCT.schedule(job1);
        System.out.println("MaxMCT算法调度结果" + maxMCTFt);

        Job job2 = new Job(nodes2, tasks2);
        MLFT mlft = new MLFT();
        double mlftFt = mlft.schedule(job2);
        System.out.println("MLFT算法调度结果" + mlftFt);

        Job job3 = new Job(nodes3, tasks3);
        CoarseGrainedSegment cgs = new CoarseGrainedSegment();
        double cgsFt = cgs.schedule(job3);
        System.out.println("粗粒度分片算法调度结果" + cgsFt);

        Job job4 = new Job(nodes4,tasks4);
        MergeMaxMCT mergeMaxMCT = new MergeMaxMCT();
        double mmmFt = mergeMaxMCT.schedule(job4);
        System.out.println("归并MaxMCT算法调度结果" + mmmFt);
    }
}
