package org.student.spark.sink;

import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.DataFlowException;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class Email extends BaseSink {

    private int smtpPort = 25;
    private String smtpHost = "";
    private String emailFrom = "";
    private String emailTo = "";
    private String emailSubject = "Mail notify";
    private String msg = "";

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {

        if (spark.catalog().tableExists(table)) {
            try {
                Dataset<String> df = spark.table(table).select("msg").as(Encoders.STRING());
                if (df.isEmpty()) {
                    return;
                } else {
                    msg = df.first();
                    if (msg.isEmpty()) {
                        logger.warn("No spark job running long !");
                        return;
                    }
                }
                CommonUtils.sendMail(smtpHost, smtpPort, emailFrom, emailTo, emailSubject, msg);
            } catch (Exception ex) {
                throw new DataFlowException(ex);
            }
        }

    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public boolean checkConfig() {
        return true;
    }

    @Override
    public void prepare(SparkSession spark) {

        table = config.getString("table");
        smtpPort = CommonUtils.getIntWithDefault(config, "smtp_port", smtpPort);
        smtpHost = CommonUtils.getStringWithDefault(config, "smtp_host", smtpHost);
        emailFrom = CommonUtils.getStringWithDefault(config, "email_from", emailFrom);
        emailTo = CommonUtils.getStringWithDefault(config, "email_to", emailTo);
        emailSubject = CommonUtils.getStringWithDefault(config, "email_subject", emailSubject);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }
}
