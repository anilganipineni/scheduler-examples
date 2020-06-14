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
package com.github.anilganipineni.scheduler.examples.base.helper;

import java.util.Objects;

/**
 * @author akganipineni
 */
public interface TaskInstanceId {
    String getTaskName();
    String getId();
    /**
     * @param name
     * @param id
     * @return
     */
    public static TaskInstanceId of(String name, String id) {
        return new StandardTaskInstanceId(name, id);
    }
    public class StandardTaskInstanceId implements TaskInstanceId {
    	private final String taskName;
        private final String id;
        /**
         * @param taskName
         * @param id
         */
        public StandardTaskInstanceId(String taskName, String id) {
            this.taskName = taskName;
            this.id = id;
        }
        /**
         * @see com.github.anilganipineni.scheduler.testhelper.TaskInstanceId#getTaskName()
         */
        @Override
        public String getTaskName() {
            return this.taskName;
        }
        /**
         * @see com.github.anilganipineni.scheduler.testhelper.TaskInstanceId#getId()
         */
        @Override
        public String getId() {
            return this.id;
        }
        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StandardTaskInstanceId that = (StandardTaskInstanceId) o;
            return Objects.equals(taskName, that.taskName) &&
                    Objects.equals(id, that.id);
        }
        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return Objects.hash(taskName, id);
        }
    }
}
