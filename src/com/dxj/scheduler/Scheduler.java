package com.dxj.scheduler;

import com.dxj.model.Job;

public interface Scheduler {
    double schedule(Job job);
}
