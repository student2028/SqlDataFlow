package org.student.spark.sql;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * *
 * convert sql to hocon (json format) used by sparkdataflow
 **/
public class SQL2HOCONParser extends DSLSQLBaseVisitor<String> {

    final static String RESULT_TABLE_NAME = "   result_table_name=";

    StringBuilder sbSet = new StringBuilder();
    StringBuilder sbLoad = new StringBuilder("source {").append(System.lineSeparator());
    StringBuilder sbSelect = new StringBuilder("processor {").append(System.lineSeparator());
    StringBuilder sbSave = new StringBuilder("sink {").append(System.lineSeparator());
    int index = 1;  //for load
    int index2 = 1; //for save
    int index3 = 1; //for processor

    public enum PREFIX {
        SET,
        LOAD,
        SELECT,
        SAVE
    }

    @Override
    public String visitSql(DSLSQLParser.SqlContext ctx) {

        String prefix = ctx.getChild(0).getText().toUpperCase();
        PREFIX eprefix = PREFIX.valueOf(prefix);

        switch (eprefix) {
            case SET:
                sbSet.append(ctx.getText().substring(3)).append(System.lineSeparator());
                break;
            case LOAD:
                extractLoad(ctx);
                break;
            case SAVE:
                extractSave(ctx);
                break;
            case SELECT:
                extractSelect(ctx);
                break;
            default:
                System.out.println("others:" + ctx.getText());
        }

        return visitChildren(ctx);
    }

    private void extractSave(DSLSQLParser.SqlContext ctx) {
        String sformat = "";
        String mode = "append";
        String table = "";
        boolean notAdd = true;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (ctx.getChild(i) instanceof DSLSQLParser.FormatContext) {
                sformat = ctx.getChild(i).getText();
                sbSave.append(" ").append(sformat).append(index2++).append("{").append(System.lineSeparator());
            } else if (ctx.getChild(i) instanceof DSLSQLParser.TableNameContext) {
                table = ctx.getChild(i).getText();
            } else if (ctx.getChild(i) instanceof DSLSQLParser.PathContext) {
                extractPath(sbSave, ctx.getChild(i), sformat, true);
            } else if (ctx.getChild(i) instanceof DSLSQLParser.ExpressionContext) {
                DSLSQLParser.ExpressionContext ec = (DSLSQLParser.ExpressionContext) ctx.getChild(i);
                sbSave.append("   ").append(ec.qualifiedName().getText()).append("=").append(qualifyText(printText(ec))).append(System.lineSeparator());
            } else if (ctx.getChild(i) instanceof DSLSQLParser.SaveModeContext) {
                mode = ctx.getChild(i).getText().toLowerCase();
            } else if (ctx.getChild(i) instanceof DSLSQLParser.BooleanExpressionContext) {
                DSLSQLParser.BooleanExpressionContext bec = (DSLSQLParser.BooleanExpressionContext) ctx.getChild(i);
                sbSave.append("   ").append(bec.expression().qualifiedName().getText()).append("=").append(qualifyText(printText(bec.expression()))).append(System.lineSeparator());
            }

            if (!table.equals("") && !sformat.equals("") && notAdd) {
                sbSave.append("   table=").append(qualifyText(table)).append(System.lineSeparator());
                sbSave.append("   savemode=").append(qualifyText(mode)).append(System.lineSeparator());
                notAdd = false;
            }
        }
        sbSave.append("   }").append(System.lineSeparator());
    }

    private void extractLoad(DSLSQLParser.SqlContext ctx) {
        String format = "";
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (ctx.getChild(i) instanceof DSLSQLParser.FormatContext) {
                format = ctx.getChild(i).getText();
                sbLoad.append(" ").append(format).append(index++).append("{").append(System.lineSeparator());
            } else if (ctx.getChild(i) instanceof DSLSQLParser.TableNameContext) {
                sbLoad.append(RESULT_TABLE_NAME).append(qualifyText(ctx.getChild(i).getText())).append(System.lineSeparator());
            } else if (ctx.getChild(i) instanceof DSLSQLParser.PathContext) {
                extractPath(sbLoad, ctx.getChild(i), format, false);
            } else if (ctx.getChild(i) instanceof DSLSQLParser.ExpressionContext) {
                DSLSQLParser.ExpressionContext ec = (DSLSQLParser.ExpressionContext) ctx.getChild(i);
                sbLoad.append("   ").append(ec.qualifiedName().getText()).append("=").append(qualifyText(printText(ec))).append(System.lineSeparator());
            } else if (ctx.getChild(i) instanceof DSLSQLParser.BooleanExpressionContext) {
                DSLSQLParser.BooleanExpressionContext bec = (DSLSQLParser.BooleanExpressionContext) ctx.getChild(i);
                sbLoad.append("   ").append(bec.expression().qualifiedName().getText()).append("=").append(qualifyText(printText(bec.expression()))).append(System.lineSeparator());
            }
        }
        sbLoad.append("   }").append(System.lineSeparator());
    }

    private String printText(DSLSQLParser.ExpressionContext ec) {
        if (ec.STRING() == null || ec.STRING().getText().isEmpty()) {
            return ec.BLOCK_STRING().getText();
        } else {
            return cleanStr(ec.STRING().getText());
        }
    }

    public String getJson() {
        sbLoad.append("}").append(System.lineSeparator());
        sbSave.append("}").append(System.lineSeparator());
        sbSelect.append("}").append(System.lineSeparator());
        return sbSet.toString() + sbLoad.toString() + sbSelect.toString() + sbSave.toString();
    }

    private String cleanStr(String str) {
        if (str.startsWith("`") || str.startsWith("\"") || (str.startsWith("'") && !str.startsWith("'''"))) {
            return str.substring(1, str.length() - 1);
        } else {
            return str;
        }
    }

    private void extractSelect(DSLSQLParser.SqlContext ctx) {
        CharStream input = ((DSLSQLLexer) ctx.start.getTokenSource())._input;
        int start = ctx.start.getStartIndex();
        int stop = ctx.stop.getStopIndex();
        String sql = input.getText(new Interval(start, stop));
        String tableName = sql.split(" ")[sql.split(" ").length - 1];
        sql = sql.replaceAll("((?i)as)[\\s|\\n]" + tableName + "\\s*\\n*$", "");
        sbSelect.append(" sql").append(index3++).append("{\n   sql=\"\"\"").append(sql).append("\"\"\"\n");
        sbSelect.append(RESULT_TABLE_NAME).append(qualifyText(tableName)).append(System.lineSeparator());
        sbSelect.append("   }").append(System.lineSeparator());
    }

    private void extractPath(StringBuilder sb, ParseTree ctx, String format, boolean isSave) {
        String path = cleanStr(ctx.getText());
        if (format.equalsIgnoreCase("cos")) {
            if (isSave) {
                sb.append(RESULT_TABLE_NAME).append(qualifyText(path)).append(System.lineSeparator());
            } else {
                sb.append("   table=").append(qualifyText(path)).append(System.lineSeparator());
            }
        } else if (format.equalsIgnoreCase("static")) {
            sb.append("   format=").append(qualifyText(path)).append(System.lineSeparator());
        } else if (format.equalsIgnoreCase("file")) {
            sb.append("   path=").append(qualifyText(path)).append(System.lineSeparator());
        } else if (format.equalsIgnoreCase("kafka")) {
            sb.append("   tag=").append(qualifyText(path)).append(System.lineSeparator());
        } else if (format.equalsIgnoreCase("jdbc")
                || format.equalsIgnoreCase("dashdb")
                || format.equalsIgnoreCase("postgre")) {
            String tag = path.split("\\.")[0];
            sb.append("   tag=").append(qualifyText(tag)).append(System.lineSeparator());
            String table = path.substring(tag.length() + 1);
            if (isSave) {
                sb.append(RESULT_TABLE_NAME).append(qualifyText(table)).append(System.lineSeparator());
            } else {
                sb.append("   table=").append(qualifyText(table)).append(System.lineSeparator());
            }
        }
    }

    private String qualifyText(String text) {
        return text.startsWith("\"") ? text : "\"" + text + "\"";
    }
}
