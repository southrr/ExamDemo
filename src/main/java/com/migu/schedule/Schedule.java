package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/*
*类名和方法不能修改
 */
public class Schedule {
    List<Node> nodes;
    List<Task> pendingTasks;


    public int init() {
        if (null == nodes) {
            nodes = new ArrayList<Node>();
        } else {
            for (Node node:nodes) {
                node.getTasks().clear();
            }
            nodes.clear();
        }

        if (null == pendingTasks) {
            pendingTasks = new ArrayList<Task>();
        } else {
            pendingTasks.clear();
        }

        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        for (Node node:nodes) {
            if (node.getNodeId() == nodeId) {
                return ReturnCodeKeys.E005;
            }
        }

        Node node = new Node(nodeId);
        nodes.add(node);

        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        for (Node node:nodes) {
            if (node.getNodeId() == nodeId) {
                nodes.remove(node);
                return ReturnCodeKeys.E006;
            }
        }
        return ReturnCodeKeys.E007;
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        Task task = new Task(taskId, consumption);
        if (pendingTasks.contains(task)) {
            return ReturnCodeKeys.E010;
        }
        for (Node node:nodes) {
            if (node.getTasks().contains(task)) {
                return ReturnCodeKeys.E010;
            }
        }
        pendingTasks.add(task);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        for(Task task:pendingTasks) {
            if (task.getTaskId() == taskId) {
                pendingTasks.remove(task);
                return ReturnCodeKeys.E011;
            }
        }

        for (Node node:nodes) {
            for (Task task:node.getTasks()) {
                if (task.getTaskId() == taskId) {
                    node.getTasks().remove(task);
                    return ReturnCodeKeys.E011;
                }
            }
        }

        return ReturnCodeKeys.E012;
    }


    public int scheduleTask(int threshold) {
        if (threshold <= 0) {
            return ReturnCodeKeys.E002;
        }

        if (nodes.size() == 0) {
            return ReturnCodeKeys.E014;
        } else {
            Collections.sort(nodes);
            for (Task task : pendingTasks) {
                nodes.get(0).addTask(task);
                Collections.sort(nodes);
            }

            if (nodes.size() <= 1) {
                return ReturnCodeKeys.E013;
            }

            boolean scheduled = false;
            for (int i = 0; i < nodes.size(); i++) {
                for (int j =1; j < nodes.size(); j++) {
                    if (nodes.get(j).getTotalConsumption() - nodes.get(i).getTotalConsumption() > threshold) {
                        scheduled = true;
                        break;
                    }
                }
            }

            if (scheduled) {
                return ReturnCodeKeys.E013;
            }

            List<Task> allTasks = new ArrayList<Task>();

            for (int i=0; i<nodes.size(); i++) {
                allTasks.addAll(nodes.get(i).getTasks());
                nodes.get(i).getTasks().clear();
            }

            for (int i = 0; i < allTasks.size(); i++) {
                for (int j = 0; j < nodes.size(); j++) {
                    if (nodes.get(j).getTasks().size() == 0|| nodes.get(j).getTasks().get(0).equals(allTasks.get(i))) {
                        nodes.get(j).getTasks().add(allTasks.get(i));
                        break;
                    }
                }
            }
            allTasks.clear();
        }

        return ReturnCodeKeys.E014;
    }

    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (null == tasks) {
            return ReturnCodeKeys.E016;
        }

        tasks.clear();

        for(Task task:pendingTasks) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setTaskId(task.getTaskId());
            taskInfo.setNodeId(-1);
            tasks.add(taskInfo);
        }

        for (Node node:nodes) {
            for (Task task:node.getTasks()) {
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setTaskId(task.getTaskId());
                taskInfo.setNodeId(node.getNodeId());
                tasks.add(taskInfo);
            }
        }

        return ReturnCodeKeys.E015;
    }

    private class Task implements Comparable<Task> {
        private int taskId;
        private int consumption;

        public Task(int taskId, int consumption) {
            this.taskId = taskId;
            this.consumption = consumption;
        }

        public int getTaskId() {
            return taskId;
        }

        public void setTaskId(int taskId) {
            this.taskId = taskId;
        }

        public int getConsumption() {
            return consumption;
        }

        public void setConsumption(int consumption) {
            this.consumption = consumption;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (null == obj) {
                return false;
            }
            if (obj instanceof Task) {
                return ((Task)obj).getTaskId() == taskId;
            }
            return false;
        }

        public int compareTo(Task o) {
            int consumptionDelta = getConsumption() - o.getConsumption();
            if (consumptionDelta == 0) {
                return getTaskId() - o.getTaskId();
            }
            return consumptionDelta;
        }
    }

    private class Node implements Comparable<Node> {
        private int nodeId;
        private List<Task> tasks;

        public Node(int nodeId) {
            this.nodeId = nodeId;
            this.tasks = new ArrayList<Task>();
        }

        public int getNodeId() {
            return nodeId;
        }

        public void setNodeId(int nodeId) {
            this.nodeId = nodeId;
        }

        public List<Task> getTasks() {
            return tasks;
        }

        public void setTasks(List<Task> tasks) {
            this.tasks = tasks;
        }

        public void addTask(Task task) {
            tasks.add(task);
        }

        int getTaskNum() {
            return tasks.size();
        }

        int getTotalConsumption() {
            int consumption = 0;
            for (Task task:tasks) {
                consumption += task.getConsumption();
            }
            return consumption;
        }

        public int compareTo(Node o) {
            int consumptionDelta = getTotalConsumption() - o.getTotalConsumption();
            if (consumptionDelta == 0) {
                int taskNumDelta = getTaskNum() - o.getTaskNum();
                if (taskNumDelta == 0) {
                    return nodeId - o.getNodeId();
                }
                return taskNumDelta;
            }
            return consumptionDelta;
        }
    }

}
