package com.belkatechnologies.bigquery.manager;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A decorator class for {@link FieldValueList} providing convenient methods to retrieve values
 * from BigQuery result sets.
 */
public class FieldValueListDecorator {

    /**
     * The decorated {@link FieldValueList}.
     */
    @Getter
    private final FieldValueList values;

    /**
     * Private constructor to create a decorator for the given {@link FieldValueList}.
     *
     * @param values The {@link FieldValueList} to decorate.
     */
    private FieldValueListDecorator(FieldValueList values) {
        this.values = values;
    }

    /**
     * Creates a new instance of {@link FieldValueListDecorator} for the given {@link FieldValueList}.
     *
     * @param values The {@link FieldValueList} to decorate.
     * @return A new {@link FieldValueListDecorator} instance.
     */
    public static FieldValueListDecorator of(FieldValueList values) {
        return new FieldValueListDecorator(values);
    }

    /**
     * Gets the long value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The long value or 0 if the field is null.
     */
    public long getLong(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? 0 : value.getLongValue();
    }

    /**
     * Gets the int value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The int value or 0 if the field is null.
     */
    public int getInt(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? 0 : Math.toIntExact(value.getLongValue());
    }

    /**
     * Gets the timestamp value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The timestamp value or 0 if the field is null.
     */
    public long getTimestamp(String field) {
        FieldValue value = getFieldValue(field);
        return value.isNull() ? 0 : value.getTimestampValue();
    }

    /**
     * Gets the timestamp value as an Instant associated with the specified field.
     *
     * @param field The name of the field.
     * @return The timestamp value as an Instant or the current time if the field is null.
     */
    public Instant getTimestampInstant(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? Instant.now() : value.getTimestampInstant();
    }

    /**
     * Gets the string value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The string value or null if the field is null.
     */
    public String getString(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? null : value.getStringValue();
    }

    /**
     * Gets a list of string values associated with the specified repeated field.
     *
     * @param field The name of the repeated field.
     * @return The list of string values or null if the field is null.
     */
    public List<String> getList(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? null : value.getRepeatedValue().stream().map(FieldValue::getStringValue).collect(Collectors.toList());
    }

    /**
     * Gets the int value at the specified index.
     *
     * @param index The index (starting from zero).
     * @return The int value or 0 if the field is null.
     */
    public int getInt(int index) {
        final FieldValue value = getFieldValue(index);
        return value.isNull() ? 0 : Math.toIntExact(value.getLongValue());
    }

    /**
     * Gets the long value at the specified index.
     *
     * @param index The index (starting from zero).
     * @return The long value or 0 if the field is null.
     */
    public long getLong(int index) {
        final FieldValue value = getFieldValue(index);
        return value.isNull() ? 0 : value.getLongValue();
    }

    /**
     * Gets the string value at the specified index.
     *
     * @param index The index (starting from zero).
     * @return The string value or null if the field is null.
     */
    public String getString(int index) {
        return values.get(index).getStringValue();
    }

    /**
     * Gets a single {@link FieldValue} from the first row of the given {@link TableResult}.
     *
     * @param result The {@link TableResult}.
     * @return The single {@link FieldValue} from the first row.
     */
    public static FieldValue getSingleValue(TableResult result) {
        return result.iterateAll().iterator().next().get(0);
    }

    /**
     * Gets the {@link FieldValue} at the specified name.
     *
     * @param field The name of the field.
     * @return The {@link FieldValue} at the specified index.
     */
    public FieldValue getFieldValue(String field) {
        return values.get(field);
    }

    /**
     * Gets the {@link FieldValue} at the specified index.
     *
     * @param index The index (starting from zero).
     * @return The {@link FieldValue} at the specified index.
     */
    public FieldValue getFieldValue(int index) {
        return values.get(index);
    }

    /**
     * Gets the byte array value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The byte array value or null if the field is null.
     */
    public byte[] getBytes(String field) {
        final FieldValue value = getFieldValue(field);
        return value.isNull() ? null : value.getBytesValue();
    }

    /**
     * Gets the boolean value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The boolean value or null if the field is null.
     */
    public Boolean getBooleanValue(String field) {
        FieldValue value = getFieldValue(field);
        return !value.isNull() && value.getBooleanValue();
    }

    /**
     * Gets the double value associated with the specified field.
     *
     * @param field The name of the field.
     * @return The double value or 0 if the field is null.
     */
    public double getDouble(String field) {
        FieldValue value = getFieldValue(field);
        return value.isNull() ? 0 : value.getNumericValue().doubleValue();
    }
}
