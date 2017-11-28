package com.dxj.util;

import com.dxj.model.Node;
import com.dxj.model.Task;

import java.util.ArrayList;
import java.util.List;

/**
 * 任务（分片）工具类
 * Created by deng on 2017/11/28.
 */
public class TaskUtil {
    private static final double rackLocalSpeed = 128.0;//同一机架，网速128MB/s
    private static final double rackCrossSpeed = 12.8;//不同机架,网速12.8MB/s

    /**
     * 分片放置，每个分片共三个副本，第一、二个副本在同一个机架，第三个副本在另一个机架上
     *
     * @param rackNodes      第一个机架结点集
     * @param otherRackNodes 第二个机架结点集
     * @return List<Node> 分片的所在的结点集合
     */
    public static List<Node> segmentPlacement(List<Node> rackNodes, List<Node> otherRackNodes) {
        int firstIndex = Random.nextInt(0, rackNodes.size()), secondIndex = Random.nextInt(0, rackNodes.size()), thirdIndex = Random.nextInt(0, otherRackNodes.size());
        while (secondIndex == firstIndex)
            secondIndex = (firstIndex + Random.nextInt(0, rackNodes.size())) % rackNodes.size();
        List<Node> nodes = new ArrayList<>();
        nodes.add(rackNodes.get(firstIndex));
        nodes.add(rackNodes.get(secondIndex));
        nodes.add(otherRackNodes.get(thirdIndex));
        return nodes;
    }

    /**
     * 任务通信开销
     *
     * @param task 任务
     * @param node 转码节点
     * @return 通信开销
     */
    public static double getCommnicationTime(Task task, Node node) {
        int nodeRackNum = node.getRack().getRackNum();
        List<Node> location = task.getLocation();
        double segmentSize = task.getSegmentSize();
        if (location.contains(node)) return 0;//同一结点
        else {
            boolean isRackLocal = false;
            for (Node localtionNode : location) {
                if (localtionNode.getRack().getRackNum() == nodeRackNum) {
                    isRackLocal = true;
                    break;
                }
            }
            if (isRackLocal) return segmentSize / rackLocalSpeed;
            else return 3 * segmentSize / rackCrossSpeed;
        }
    }
}
