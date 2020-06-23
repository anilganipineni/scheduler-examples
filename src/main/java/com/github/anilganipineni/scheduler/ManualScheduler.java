/**
 * Copyright (C) Anil Ganipineni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.anilganipineni.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.anilganipineni.scheduler.SchedulerImpl;
import com.github.anilganipineni.scheduler.SchedulerName;
import com.github.anilganipineni.scheduler.StatsRegistry;
import com.github.anilganipineni.scheduler.TaskResolver;
import com.github.anilganipineni.scheduler.Waiter;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.examples.base.helper.SettableClock;
import com.github.anilganipineni.scheduler.task.Task;

/**
 * @author akganipineni
 */
public class ManualScheduler extends SchedulerImpl {
    /**
     * The <code>Logger</code> instance for this class.
     */
	private static final Logger logger = LogManager.getLogger(ManualScheduler.class);
    private final SettableClock clock;

    public ManualScheduler(SettableClock clock, SchedulerRepository<ScheduledTasks> taskRepository, TaskResolver taskResolver, int maxThreads, ExecutorService executorService, SchedulerName schedulerName, Waiter waiter, Duration heartbeatInterval, boolean executeImmediately, StatsRegistry statsRegistry, int pollingLimit, Duration deleteUnresolvedAfter, List<Task> onStartup) {
        super(clock, taskRepository, taskResolver, maxThreads, executorService, schedulerName, waiter, heartbeatInterval, executeImmediately, statsRegistry, pollingLimit, deleteUnresolvedAfter, onStartup);
        this.clock = clock;
    }

    public SettableClock getClock() {
        return clock;
    }

    public void tick(Duration moveClockForward) {
        clock.set(clock.now.plus(moveClockForward));
    }

    public void setTime(Instant newtime) {
        clock.set(newtime);
    }

    public void runAnyDueExecutions() {
        super.executeDue();
    }

    public void runDeadExecutionDetection() {
        super.detectDeadExecutions();
    }


    public void start() {
        logger.info("Starting manual scheduler. Executing on-startup tasks.");
        executeInitialTasks();
    }
}
