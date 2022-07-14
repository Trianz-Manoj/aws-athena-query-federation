/*-
 * #%L
 * trianz-googlesheets-athena-google
 * %%
 * Copyright (C) 2019 - 2021 Trianz
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.trianz.athena.connectors.kinesis;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Utilities that help with Sql operations.
 */
public class AmazonKinesisSqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonKinesisSqlUtils.class);

    private static final String GOOGLESHEET_QUOTE_CHAR = "'";


    /**
     * Builds an SQL statement from the schema, table name, split and contraints that can be executable by
     * BigQuery.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information to add as a constraint.
     * @param parameterValues Query parameter values for parameterized query.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */

    public static String buildSqlFromSplit(TableName tableName,
                                           Schema schema,
                                           Constraints constraints,
                                           Split split,
                                           List parameterValues)
    {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");
        Map<String,String> limitAndOffsets = split.getProperties();
        StringJoiner sj = new StringJoiner(",");

        if (schema.getFields().isEmpty()) {
            sj.add("null");
        }
        else {
            for (Field field : schema.getFields()) {
                sj.add(field.getName());
            }
        }
        sqlBuilder.append(sj.toString())
                .append(" from ")
                .append(tableName.getSchemaName())
                .append(".")
                .append(tableName.getTableName());

        System.out.println( " STEP 5.0 " + sqlBuilder.toString());
        System.out.println( " STEP 5.1 " + sj.toString());

        List<String> clauses = toConjuncts(schema.getFields(), constraints, split.getProperties(), parameterValues);

        System.out.println( " STEP 5.0.0 " +  clauses);

        if (!clauses.isEmpty()) {
            sqlBuilder.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        System.out.println( " STEP 5.2 " + sqlBuilder.toString());


        return sqlBuilder.toString();
    }


    private static List<String> toConjuncts(List<Field> columns,
                                            Constraints constraints,
                                            Map<String, String> partitionSplit,
                                            List parameterValues)
    {
        System.out.println(" --- STEP 5.3 : paramvalues : " + parameterValues.toString());

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            ArrowType type = column.getType();
            System.out.println( " -- STEP 5.4 ArrowType: " + type );

            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    System.out.println( " -- STEP 5.5 " + valueSet.toString() );
                    builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                }
            }
        }
        return builder.build();
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type,
                                      List<String> parameterValues)
    {
        parameterValues.add(getValueForWhereClause(columnName, value, type));
        return columnName + " " + operator + quote( value.toString() ) ;
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<String> parameterValues)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", columnName);
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, parameterValues));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(getValueForWhereClause(columnName, value, type));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "?"));
                disjuncts.add( columnName + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    //Gets the representation of a value that can be used in a where clause, ie String values need to be quoted, numeric doesn't.
    private static String getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {

        System.out.println( " STEP 6.0 ArrowType: " + arrowType.getTypeID());
        String val;
        StringBuilder tempVal;
        switch (arrowType.getTypeID()) {
            case Int:
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            case FloatingPoint:
            case Bool:
            case Utf8:
                System.out.println( " STEP 6.0.1 UTF8 type ");
                return value.toString();
            case Date:
            case Time:
            case Timestamp:
            case Interval:
            case Binary:
            case FixedSizeBinary:
            case Null:
            case Struct:
            case List:
            case FixedSizeList:
            case Union:
            case NONE:
                return value.toString();
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }

    /**
     * Builds an SQL statement from the schema, table name, split and contraints that can be executable by
     * BigQuery.
     *
     * @param //tableName The table name of the table we are querying.
     * @param //schema The schema of the table that we are querying.
     * @param //constraints The constraints that we want to apply to the query.
     * @param //split The split information to add as a constraint.
     * @param //parameterValues Query parameter values for parameterized query.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    private static String quote(final String identifier)
    {
        return GOOGLESHEET_QUOTE_CHAR + identifier + GOOGLESHEET_QUOTE_CHAR;
    }
}
