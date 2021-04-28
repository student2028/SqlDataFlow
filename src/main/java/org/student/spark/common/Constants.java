package org.student.spark.common;

public class Constants {

    public static final String OPTIONS_PREFIX ="options.";
    public static final String JDBC_CONNECTION_TIMEOUT = "app.jdbc.connection.timeout";
    public static final String K8S_SECRETS_DIR = "app.k8s.secrets.dir";
    public static final String K8S_SECRETS_FILE = "app.k8s.secrets.file";
    public static final String K8S_SECRETS_ENABLED = "app.k8s.secrets.enabled";
    public static final String SQL_DIR = "app.sql.dir";

    public static final String JDBC_PREFIX = "app.jdbc.";
    public static final String BIGDATA_ENV = "bigdata.environment";

    public static final String queryTableColumns =
            "SELECT TABNAME,COLNAME,COLNO,TYPENAME,LENGTH,SCALE,NULLS"
                    + " FROM SYSCAT.COLUMNS\n"
                    + " WHERE UPPER('{{sourceTable}}') = TRIM(TABSCHEMA) || '.' || TRIM(TABNAME)";

    public static final String REGEXP_LENGTH = ".*\\(\\b(\\d+)\\b\\)";
    public static final String REGEXP_NUMBER = "(\\d+)";


    public static final String MERGE_TO_DASH_STMT="MERGE INTO %s AS T\n" +
            "USING ( VALUES(%s) ) \n" +
            "AS S(%s)\n" +
            "ON %s\n" +
            "WHEN MATCHED AND S.A_ENTTYP NOT IN ('DL','UB')\n" +
            "THEN UPDATE SET\n" +
            "%s \n" +
            "WHEN MATCHED AND S.A_ENTTYP IN ('DL','UB') THEN DELETE\n" +
            "WHEN NOT MATCHED AND S.A_ENTTYP NOT IN ('DL','UB')\n" +
            "THEN INSERT (%s)\n" +
            "VALUES (%s);\n";

}
