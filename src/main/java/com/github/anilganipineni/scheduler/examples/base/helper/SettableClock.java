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

import java.time.Instant;

import com.github.anilganipineni.scheduler.schedule.Clock;

/**
 * @author akganipineni
 */
public class SettableClock implements Clock {
    public Instant now = Instant.now();
    /**
     * @see com.github.anilganipineni.scheduler.Clock#now()
     */
    @Override
    public Instant now() {
        return getNow();
    }
    /**
     * @param newNow
     */
    public void set(Instant newNow) {
    	setNow(newNow);
    }
	/**
	 * @return the now
	 */
	public Instant getNow() {
		return now;
	}
	/**
	 * @param now the now to set
	 */
	public void setNow(Instant now) {
		this.now = now;
	}
}
