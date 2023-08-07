/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.geedgenetworks.utils;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple parser for determining the type of command and its parameters.
 */
final class SqlCommandParserUtils {

	private SqlCommandParserUtils() {}

	static List<SqlCommandCall> parse(List<String> lines) {
		List<SqlCommandCall> calls = new ArrayList<>();
		StringBuilder stmt = new StringBuilder();
		for (String line : lines) {
			if (line.trim().isEmpty() || line.startsWith("--")) {
				// skip empty line and comment line
				continue;
			}
			stmt.append("\n").append(line);
			if (line.trim().endsWith(";")) {
				Optional<SqlCommandCall> optionalCall = parsesqllist(stmt.toString());
				if (optionalCall.isPresent()) {
					calls.add(optionalCall.get());
				} else {
					throw new RuntimeException("Unsupported command '" + stmt.toString() + "'");
				}
				// clear string builder
				stmt.setLength(0);
			}
		}
		return calls;
	}

	static List<SqlCommandCall> parse(String sqlquery) {
		List<SqlCommandCall> calls = new ArrayList<>();
		String[] lines = sqlquery.split(";");
		for (String sql : lines) {
			if (!sql.trim().isEmpty()) {
				Optional<SqlCommandCall> optionalCall = parsesql(sql.trim());
				if (optionalCall.isPresent()) {
					calls.add(optionalCall.get());
				} else {
					throw new RuntimeException("Unsupported command '" + sql + "'");
				}
			}
		}
		return calls;
	}

	private static Optional<SqlCommandCall> parsesqllist(String stmt) {
		// normalize
		stmt = stmt.trim();
		// remove ';' at the end
		if (stmt.endsWith(";")) {
			stmt = stmt.substring(0, stmt.length() - 1).trim();
		}

		return parseSqlString(stmt);
	}

	private static Optional<SqlCommandCall> parsesql(String sql) {
		return parseSqlString(sql);
	}

	private static Optional<SqlCommandCall> parseSqlString(String sql){
		for (SqlCommand cmd : SqlCommand.values()) {
			final Matcher matcher = cmd.pattern.matcher(sql);
			if (matcher.matches()) {
				final String[] groups = new String[matcher.groupCount()];
				for (int i = 0; i < groups.length; i++) {
					groups[i] = matcher.group(i + 1);
				}
				return cmd.operandConverter.apply(groups)
						.map((operands) -> new SqlCommandCall(cmd, operands));
			}
		}
		return Optional.empty();
	}

	// --------------------------------------------------------------------------------------------

	private static final Function<String[], Optional<String[]>> NO_OPERANDS =
		(operands) -> Optional.of(new String[0]);

	private static final Function<String[], Optional<String[]>> SINGLE_OPERAND =
		(operands) -> Optional.of(new String[]{operands[0]});

	private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;

	/**
	 * Supported SQL commands.
	 */
	public enum SqlCommand {
		/**
		 * sql任务类型，目前支持：
		 * insert into、create table、create view、create temporary、set
		 */
		INSERT_INTO(
			"(INSERT\\s+INTO.*)",
			SINGLE_OPERAND),

		CREATE_TABLE(
			"(CREATE\\s+TABLE.*)",
				SINGLE_OPERAND),

		CREATE_VIEW(
				"(CREATE\\s+VIEW.*)",
				SINGLE_OPERAND),
		CREATE_TEMPORARY_VIEW(
				"(CREATE\\s+TEMPORARY.*)",
				SINGLE_OPERAND),
		SET(
			// whitespace is only ignored on the left side of '='
			"SET(\\s+(\\S+)\\s*=(.*))?",
			(operands) -> {
				if (operands.length < 3) {
					return Optional.empty();
				} else if (operands[0] == null) {
					return Optional.of(new String[0]);
				}
				return Optional.of(new String[]{operands[1], operands[2]});
			});

		public final Pattern pattern;
		public final Function<String[], Optional<String[]>> operandConverter;

		SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
			this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
			this.operandConverter = operandConverter;
		}

		@Override
		public String toString() {
			return super.toString().replace('_', ' ');
		}

		public boolean hasOperands() {
			return operandConverter != NO_OPERANDS;
		}
	}

	/**
	 * Call of SQL command with operands and command type.
	 */
	public static class SqlCommandCall {
		final SqlCommand command;
		final String[] operands;

		SqlCommandCall(SqlCommand command, String[] operands) {
			this.command = command;
			this.operands = operands;
		}

		public SqlCommandCall(SqlCommand command) {
			this(command, new String[0]);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SqlCommandCall that = (SqlCommandCall) o;
			return command == that.command && Arrays.equals(operands, that.operands);
		}

		@Override
		public int hashCode() {
			int result = Objects.hash(command);
			result = 31 * result + Arrays.hashCode(operands);
			return result;
		}

		@Override
		public String toString() {
			return command + "(" + Arrays.toString(operands) + ")";
		}
	}
}
