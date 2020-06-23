package com.github.anilganipineni.scheduler;

import java.util.Arrays;
import java.util.List;

import com.github.anilganipineni.scheduler.dao.DbUtils;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.task.Task;

/**
 * @author akganipineni
 */
public class SchedulerClientBuilder {
    private final SchedulerDataSource dataSource;
    private final TaskResolver taskResolver;
    private final SchedulerName schedulerName;
	/**
	 * @param dataSource
	 * @param knownTasks
	 */
	private SchedulerClientBuilder(SchedulerDataSource dataSource, List<Task> knownTasks) {
		this(dataSource, new TaskResolver(StatsRegistry.NOOP, knownTasks), new SchedulerName.Fixed());
    }
    /**
	 * @param dataSource
	 * @param taskResolver
	 * @param schedulerName
	 */
	private SchedulerClientBuilder(SchedulerDataSource dataSource, TaskResolver taskResolver, SchedulerName schedulerName) {
		this.dataSource = dataSource;
		this.taskResolver = taskResolver;
		this.schedulerName = schedulerName;
	}
	/**
     * @param dataSource
     * @param knownTasks
     * @return
     */
    public static SchedulerClientBuilder create(SchedulerDataSource dataSource, Task ... knownTasks) {
        return new SchedulerClientBuilder(dataSource, Arrays.asList(knownTasks));
    }
    /**
     * @param dataSource
     * @param knownTasks
     * @return
     */
    public static SchedulerClientBuilder create(SchedulerDataSource dataSource, List<Task> knownTasks) {
        return new SchedulerClientBuilder(dataSource, knownTasks);
    }
    /**
     * @return
     */
    public Scheduler build() {
        return new StandardSchedulerClient(DbUtils.getRepository(dataSource, taskResolver, schedulerName));
    }
}
