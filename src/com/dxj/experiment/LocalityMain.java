package com.dxj.experiment;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;
import com.dxj.scheduler.*;
import com.dxj.scheduler.locality.PLTS;
import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;

/**
 * 考虑分片的通信开销和数据本地性
 */
public class LocalityMain {

    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<>(), nodes1 = new ArrayList<>(), nodes2 = new ArrayList<>(), nodes3 = new ArrayList<>(), nodes4 = new ArrayList<>();
        List<Task> tasks = new ArrayList<>(), tasks1 = new ArrayList<>(), tasks2 = new ArrayList<>(), tasks3 = new ArrayList<>(), tasks4 = new ArrayList<>();

        int rackCount = 3;
        List<Rack> racks = new ArrayList<>();
        for (int i = 0; i < rackCount; i++) {
            racks.add(new Rack(i));
        }

        for (int i = 0; i < 25; i++) {
            double capacity = Random.nextDouble(1.0, 3.0);
            Rack rack = racks.get(Random.nextInt(0, rackCount));//随机选取一个机架

            Node node = new Node("node_" + i, capacity, rack);
            nodes.add(node);
            List<Node> rackNodes = rack.getNodes();
            rackNodes.add(node);
            rack.setNodes(rackNodes);

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

            int rackNum = Random.nextInt(0, rackCount),otherRackNum = Random.nextInt(0, rackCount);
            while (rackNum == otherRackNum) otherRackNum = Random.nextInt(0, rackCount);

            Rack rack = racks.get(rackNum), otherRack = racks.get(otherRackNum);
            List<Node> rackNodes = rack.getNodes(), otherRackNodes = otherRack.getNodes();
            List<Node> location = segmentPlacement(rackNodes, otherRackNodes);

            Task task = new Task("task_" + i, complexity, location);
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
        PLTS plts = new PLTS();
        double pltsFt = plts.schedule(job);
        System.out.println(pltsFt);
    }

    /**
     * 分片放置，每个分片共三个副本，第一、二个副本在同一个机架，第三个副本在另一个机架上
     *
     * @param rackNodes      第一个机架结点集
     * @param otherRackNodes 第二个机架结点集
     * @return List<Node> 分片的所在的结点集合
     */
    public static List<Node> segmentPlacement(List<Node> rackNodes, List<Node> otherRackNodes) {
        int firstIndex = Random.nextInt(0, rackNodes.size()), secondIndex = Random.nextInt(0, rackNodes.size()), thirdIndex = Random.nextInt(0, otherRackNodes.size());
        while (secondIndex == firstIndex) secondIndex = Random.nextInt(0, rackNodes.size());
        List<Node> nodes = new ArrayList<>();
        nodes.add(rackNodes.get(firstIndex));
        nodes.add(rackNodes.get(secondIndex));
        nodes.add(otherRackNodes.get(thirdIndex));
        return nodes;
    }
}
