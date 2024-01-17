package com.belkatechnologies.bigquery.utils;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Column {
    private String name;

    private String tableAlias;

    private String type;

    public String getColumnAlias() {
        return tableAlias + "_" + name;
    }

    public String getColumnNameWithAlias() {
        return tableAlias + "." + name;
    }
}