/**
 * Copyright (C) Gustav Karlsson
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
package com.github.anilganipineni.scheduler.examples.base;

import java.time.Duration;

import com.github.anilganipineni.scheduler.ExecutionContext;
import com.github.anilganipineni.scheduler.dao.ScheduledTasks;
import com.github.anilganipineni.scheduler.schedule.Schedule;
import com.github.anilganipineni.scheduler.task.OneTimeTask;
import com.github.anilganipineni.scheduler.task.RecurringTask;
import com.github.anilganipineni.scheduler.task.Task;
import com.github.anilganipineni.scheduler.task.handler.CompletionHandler;
import com.github.anilganipineni.scheduler.task.handler.FailureHandler;
import com.github.anilganipineni.scheduler.task.handler.OnFailureRetryLater;
import com.github.anilganipineni.scheduler.task.handler.ReviveDeadExecution;
import com.github.anilganipineni.scheduler.task.handler.VoidExecutionHandler;

@Deprecated
public class ComposableTask {
    /**
     * @param name
     * @param schedule
     * @param executionHandler
     * @return
     */
    public static RecurringTask recurringTask(String name, Schedule schedule, VoidExecutionHandler executionHandler) {
        return new RecurringTask(name, schedule) {
			@Override
			public void executeActual(ScheduledTasks task, ExecutionContext context) {
                executionHandler.execute(task, context);
			}
        };
    }
    /**
     * @param name
     * @param executionHandler
     * @return
     */
    public static  OneTimeTask onetimeTask(String name, VoidExecutionHandler executionHandler) {
        return new OneTimeTask(name) {
            @Override
			public void executeActual(ScheduledTasks task, ExecutionContext context) {
                executionHandler.execute(task, context);
			}
        };
    }
    /**
     * @param name
     * @param completionHandler
     * @param executionHandler
     * @return
     */
    public static  Task customTask(String name, CompletionHandler completionHandler, VoidExecutionHandler executionHandler) {
        return new Task(name, new OnFailureRetryLater(Duration.ofMinutes(5)), new ReviveDeadExecution()) {
            @Override
			public CompletionHandler execute(ScheduledTasks task, ExecutionContext context) {
                executionHandler.execute(task, context);
                return completionHandler;
            }
        };
    }
    /**
     * @param name
     * @param completionHandler
     * @param failureHandler
     * @param executionHandler
     * @return
     */
    public static  Task customTask(String name, CompletionHandler completionHandler, FailureHandler failureHandler, VoidExecutionHandler executionHandler) {
        return new Task(name, failureHandler, new ReviveDeadExecution()) {
            @Override
			public CompletionHandler execute(ScheduledTasks task, ExecutionContext context) {
                executionHandler.execute(task, context);
                return completionHandler;
			}
        };
    }

}
