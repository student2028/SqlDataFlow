package org.student.spark.sql;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class SQL2HOCONDriver {

    public static String getJson(String sql) {
        CaseChangingCharStream input = new CaseChangingCharStream(sql);
        DSLSQLLexer lexer = new DSLSQLLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        DSLSQLParser parser = new DSLSQLParser(tokens);
        ParseTree tree = parser.statement();
        SQL2HOCONParser sql2HoconParser = new SQL2HOCONParser();
        sql2HoconParser.visit(tree);
        return sql2HoconParser.getJson();
    }

    public static void main(String[] args) {

        String sql = "" +
                "SET  bigdata.environment=\"\";" +
                "load file.`/tmp/test.csv` as table0;" +
                "LOAD jdbc.`db.table` " +
                "as table1;\n" +
                "select * from table1 as output;\n" +
                "select *,1 as id from output as output2;" +
                "save APPEND output as jdbc.`tag1.table2`;" +
                "SAVE overwrite output2 as jdbc.`tag2.table3`;";
       // System.out.println(getJson(sql));

        String sql2 ="select * from test.test as test1;" +
                "save append test1 as stdout.tt options limit=\"10\";";
        //System.out.println(getJson(sql2));

        String sql3="set  bigdata.environment=\"\"\n" +
                "load file.`/Users/student2020/code/student/SparkDataFlow/src/test/resources/test-data/topic_version.csv`" +
                " options format=\"csv\"" +
                " and options.header=\"true\" as test1;\n" +
                "select *,1 as tid from test1 as test2;" +
                "save append test2 as stdout.test3;";
        //System.out.println(getJson(sql3));

        String sql4 = "set  bigdata.environment=\"\"\n" +
                "load file.`/Users/student2020/code/student/SparkDataFlow/src/test/resources/test-data/topic_version.csv` options format=\"csv\"\n" +
                "and options.header=\"true\" as test1;\n" +
                "select *,1 as tid from test1  as test2;\n" +
                "save append test2 as stdout.test3 options limit=\"5\";\n" +
                "save append test2 as jdbc.`reconqa.test.topic_version`;";
       // System.out.println(getJson(sql4));

        String sql5 ="set  bigdata.environment=\"\"\n" +
                "load static.`csv` options data=\"\"\"\n" +
                "table1,101\n" +
                "table2,102\n" +
                "table3,103\n" +
                "\"\"\"   as test1;\n" +
                "save append test1 stdout.testx;\n";
        System.out.println(getJson(sql5));


    }

}
