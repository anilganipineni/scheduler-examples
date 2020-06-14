package com.github.anilganipineni.scheduler.examples;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.anilganipineni.scheduler.Scheduler;
import com.github.anilganipineni.scheduler.SchedulerBuilder;
import com.github.anilganipineni.scheduler.dao.SchedulerDataSource;
import com.github.anilganipineni.scheduler.schedule.FixedDelay;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.TaskFactory;

/**
 * @author akganipineni
 */
public class Tester {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		startScheduler(new SchedulerDataSourceImpl());
	}
	/**
	 * 
	 */
	private static void startScheduler(SchedulerDataSource dataSource) {
		Task hourlyTask = TaskFactory.recurring("my-hourly-task", FixedDelay.ofHours(6))
		        .execute((task, ctx) -> {
		        	System.out.println(new Date() + " - Anil Hourly Scheduler Executed..........");
		        });
		
		Task minutesTask = TaskFactory.recurring("my-minutes-task", FixedDelay.ofMinutes(5))
		        .execute((task, ctx) -> {
		        	System.out.println(new Date() + " - Anil Minutes Scheduler Executed..........");
		        });
		
		System.out.println(new Date() + " - Enabling the Anil Scheduler..........");
		
		List<Task> startTasks = Arrays.asList(hourlyTask, minutesTask);
		Scheduler s = SchedulerBuilder.create(dataSource).threads(5).enableImmediateExecution().startTasks(startTasks).build();

		// Task(s) will automatically scheduled on startup if not already started (i.e. if not exists in the db)
		s.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    @Override
		    public void run() {
				System.out.println(new Date() + " - Received shutdown signal.");
				if(s != null) {
					s.stop();
				}
		    }
		});
		new Thread(new OneTimeTaskInitiater(s), "New Ontetime Task").start();
	}
	/**
	 * @author akganipineni
	 */
	private static class OneTimeTaskInitiater implements Runnable {
		private Scheduler s = null;
		/**
		 * @param s
		 */
		public OneTimeTaskInitiater(Scheduler s) {
			this.s = s;
		}
		/**
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Start..........!");
			try {
				Thread.sleep(1 * 60 * 1000);
				System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - After Sleep..........!");

				Task oneTimeTask = TaskFactory.oneTime("my-onetime-task")
		                .execute((task, ctx) -> {
		                	System.out.println(new Date() + " - Anil One Time Scheduler Executed and Custom data Id : " + task.getId());
		                });

				Map<String, Object> map = new HashMap<String, Object>();
				map.put("id", Long.parseLong("1001"));
				s.addTask(oneTimeTask);

					s.schedule(oneTimeTask.instance("1045", map), Instant.now().plusSeconds(600));
				System.out.println(new Date() + " - " + Thread.currentThread().getName() + " - Thread completed and scheduleed the one time task!");
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}	
	}
}
