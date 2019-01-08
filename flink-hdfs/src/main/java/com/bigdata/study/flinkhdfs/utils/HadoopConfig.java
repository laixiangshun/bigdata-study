package com.bigdata.study.flinkhdfs.utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 项目名  data-acquisition-dataflow
 * Created by zhongdev.
 * Created at 2017/10/26
 * 描述:hadoop的配置信息
 */
@Configuration
@ConfigurationProperties(prefix = "hadoop")
public class HadoopConfig {

    private String fsDefaultFS;
    private String hadoopTmpDir;
    private Integer dfsReplication;
    private String dfsNamenodeNameDir;
    private String dfsDatanodeDataDir;
    private Boolean dfsPermissions;
    private Boolean dfsSupportAppend;
    private String dfsUser = "hdfs";

    public String getFsDefaultFS() {
        return fsDefaultFS;
    }

    public void setFsDefaultFS(String fsDefaultFS) {
        this.fsDefaultFS = fsDefaultFS;
    }

    public String getHadoopTmpDir() {
        return hadoopTmpDir;
    }

    public void setHadoopTmpDir(String hadoopTmpDir) {
        this.hadoopTmpDir = hadoopTmpDir;
    }

    public Integer getDfsReplication() {
        return dfsReplication;
    }

    public void setDfsReplication(Integer dfsReplication) {
        this.dfsReplication = dfsReplication;
    }

    public String getDfsNamenodeNameDir() {
        return dfsNamenodeNameDir;
    }

    public void setDfsNamenodeNameDir(String dfsNamenodeNameDir) {
        this.dfsNamenodeNameDir = dfsNamenodeNameDir;
    }

    public String getDfsDatanodeDataDir() {
        return dfsDatanodeDataDir;
    }

    public void setDfsDatanodeDataDir(String dfsDatanodeDataDir) {
        this.dfsDatanodeDataDir = dfsDatanodeDataDir;
    }

    public Boolean getDfsPermissions() {
        return dfsPermissions;
    }

    public void setDfsPermissions(Boolean dfsPermissions) {
        this.dfsPermissions = dfsPermissions;
    }

    public Boolean getDfsSupportAppend() {
        return dfsSupportAppend;
    }

    public void setDfsSupportAppend(Boolean dfsSupportAppend) {
        this.dfsSupportAppend = dfsSupportAppend;
    }

    public String getDfsUser() {
        return dfsUser;
    }

    public void setDfsUser(String dfsUser) {
        this.dfsUser = dfsUser;
    }
}
