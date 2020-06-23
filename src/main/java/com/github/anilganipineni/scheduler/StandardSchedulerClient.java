package com.github.anilganipineni.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.dao.SchedulerRepository;
import com.github.anilganipineni.scheduler.exception.SchedulerException;
import com.github.anilganipineni.scheduler.task.Task;

/**
 * @author akganipineni
 */
public class StandardSchedulerClient implements Scheduler {

    private static final Logger LOG = LoggerFactory.getLogger(StandardSchedulerClient.class);
    protected final SchedulerRepository<ScheduledTasks> taskRepository;
    private SchedulerEventListener schedulerClientEventListener;

    StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository) {
        this(taskRepository, SchedulerEventListener.NOOP);
    }

    StandardSchedulerClient(SchedulerRepository<ScheduledTasks> taskRepository, SchedulerEventListener schedulerClientEventListener) {
        this.taskRepository = taskRepository;
        this.schedulerClientEventListener = schedulerClientEventListener;
    }

    @Override
    public void schedule(ScheduledTasks taskId, Instant executionTime) {
        boolean success = taskRepository.createIfNotExists(new ScheduledTasks(executionTime, taskId.getTaskName(), taskId.getTaskId(), taskId.getTaskData()));
        if (success) {
            notifyListeners(EventType.SCHEDULE, taskId, executionTime);
        }
    }

    @Override
    public void reschedule(ScheduledTasks task, Instant newExecutionTime) {
        reschedule(task, newExecutionTime, null);
    }

    @Override
    public void reschedule(ScheduledTasks task, Instant newExecutionTime, Map<String, Object> newData) {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not reschedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            boolean success = taskRepository.reschedule(execution.get(), newExecutionTime, null, null, 0, newData);

            if (success) {
                notifyListeners(EventType.RESCHEDULE, task, newExecutionTime);
            }
        } else {
            throw new RuntimeException(String.format("Could not reschedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }

    @Override
    public void cancel(ScheduledTasks task) {
        String taskName = task.getTaskName();
        String instanceId = task.getTaskId();
        Optional<ScheduledTasks> execution = getExecution(taskName, instanceId);
        if(execution.isPresent()) {
            if(execution.get().isPicked()) {
                throw new RuntimeException(String.format("Could not cancel schedule, the execution with name '%s' and id '%s' is currently executing", taskName, instanceId));
            }

            taskRepository.remove(execution.get());
            notifyListeners(EventType.CANCEL, task, execution.get().getExecutionTime());
        } else {
            throw new RuntimeException(String.format("Could not cancel schedule - no task with name '%s' and id '%s' was found." , taskName, instanceId));
        }
    }

    @Override
    public void getScheduledExecutions(Consumer<ScheduledTasks> consumer) {
        try {
			taskRepository.getScheduledExecutions(consumer);
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    @Override
    public void getScheduledExecutionsForTask(String taskName, Consumer<ScheduledTasks> consumer) {
        try {
			taskRepository.getScheduledExecutions(taskName, consumer);
		} catch (SchedulerException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
    }

    @Override
    public Optional<ScheduledTasks> getScheduledExecution(ScheduledTasks task) {
        Optional<ScheduledTasks> e = getExecution(task.getTaskName(), task.getTaskId());
        return e;
    }

    private void notifyListeners(EventType eventType, ScheduledTasks task, Instant executionTime) {
        try {
            schedulerClientEventListener.newEvent(new EventContext(eventType, task.getTaskName(), task.getTaskId(), task.getTaskData(), executionTime));
        } catch (Exception e) {
            LOG.error("Error when notifying SchedulerEventListener.", e);
        }
    }
    /**
     * @param taskName
     * @param task
     * @return
     */
    public Optional<ScheduledTasks> getExecution(String taskName, String task) {
    	try {
			return taskRepository.getExecution(taskName, task);
		} catch (SchedulerException ex) {
            throw new RuntimeException(ex.getMessage(), ex); // TODO : Why runtime exception
		}
    }

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#start()
	 */
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#stop()
	 */
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#addTask(com.github.anilganipineni.scheduler.task.Task)
	 */
	@Override
	public void addTask(Task task) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#executeDue()
	 */
	@Override
	public void executeDue() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#getCurrentlyExecuting()
	 */
	@Override
	public List<CurrentlyExecuting> getCurrentlyExecuting() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#detectDeadExecutions()
	 */
	@Override
	public void detectDeadExecutions() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.github.anilganipineni.scheduler.Scheduler#getFailingExecutions(java.time.Duration)
	 */
	@Override
	public List<ScheduledTasks> getFailingExecutions(Duration failingAtLeastFor) throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}
}
