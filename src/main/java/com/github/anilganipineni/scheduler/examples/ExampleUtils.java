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
package com.github.anilganipineni.scheduler.examples;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;

import javax.sql.DataSource;

import com.github.anilganipineni.scheduler.dao.JdbcRunner;
import com.github.anilganipineni.scheduler.dao.JdbcTaskRepository;
import com.github.anilganipineni.scheduler.dao.Mappers;
import com.github.anilganipineni.scheduler.dao.PreparedStatementSetter;
import com.google.common.io.CharStreams;

/**
 * @author akganipineni
 */
public class ExampleUtils {
    public static void clearTables(DataSource dataSource) {
        new JdbcRunner(dataSource).execute("delete from " + JdbcTaskRepository.TABLE_NAME, PreparedStatementSetter.NOOP);
    }

    public static Consumer<DataSource> runSqlResource(String resource) {
        return dataSource -> {

            final JdbcRunner jdbcRunner = new JdbcRunner(dataSource);
            try {
                final String statements = CharStreams.toString(new InputStreamReader(ExampleUtils.class.getResourceAsStream(resource)));
                jdbcRunner.execute(statements, PreparedStatementSetter.NOOP);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static int countExecutions(DataSource dataSource) {
        return new JdbcRunner(dataSource).execute("select count(*) from " + JdbcTaskRepository.TABLE_NAME,
            PreparedStatementSetter.NOOP, Mappers.SINGLE_INT);
    }
}
