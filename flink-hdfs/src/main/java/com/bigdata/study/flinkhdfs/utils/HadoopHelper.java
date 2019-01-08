package com.bigdata.study.flinkhdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Hadoop工具类
 *
 * @author 赵少泉
 * @date 2017年7月7日 上午10:07:09
 */
public class HadoopHelper {
    private static final Logger logger = LoggerFactory.getLogger(HadoopHelper.class);
    public Configuration config = new Configuration();
    private FileSystem hdfs;

    public HadoopHelper(HadoopConfig hadoopConfig) throws IOException {
        if (hadoopConfig.getHadoopTmpDir() != null) {
            config.set("hadoop.tmp.dir", hadoopConfig.getHadoopTmpDir());
        }
        if (hadoopConfig.getDfsReplication() != null) {
            config.set("dfs.replication", String.valueOf(hadoopConfig.getDfsReplication()));
        }
        if (hadoopConfig.getDfsNamenodeNameDir() != null) {
            config.set("dfs.namenode.name.dir", hadoopConfig.getDfsNamenodeNameDir());
        }
        if (hadoopConfig.getDfsDatanodeDataDir() != null) {
            config.set("dfs.datanode.data.dir", hadoopConfig.getDfsDatanodeDataDir());
        }
        if (hadoopConfig.getDfsPermissions() != null) {
            config.set("dfs.permissions", String.valueOf(hadoopConfig.getDfsPermissions()));
        }
        if (hadoopConfig.getDfsSupportAppend() != null) {
            config.set("dfs.support.append", String.valueOf(hadoopConfig.getDfsSupportAppend()));
        }

        if (hadoopConfig.getFsDefaultFS() != null) {
            String fsDefaultFS = hadoopConfig.getFsDefaultFS();
            if (fsDefaultFS.contains(",")) {
                String[] fsDefaultFSes = fsDefaultFS.split(",");
                for (String fsDefaultFSe : fsDefaultFSes) {
                    config.set("fs.defaultFS", fsDefaultFSe.trim());
                    try {
                        hdfs = FileSystem.get(FileSystem.getDefaultUri(config), config, hadoopConfig.getDfsUser());
                        hdfs.getStatus();
                        logger.info("fs.defaultFS 的value值是:" + fsDefaultFSe);
                        break;
                    } catch (Throwable e) {
                        logger.error(e.getMessage());
                    }
                }
            } else {
                config.set("fs.defaultFS", fsDefaultFS);
                try {
                    hdfs = FileSystem.get(FileSystem.getDefaultUri(config), config, hadoopConfig.getDfsUser());
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public Configuration getConfig() {
        return config;
    }

    public HadoopHelper() throws IOException {
        hdfs = FileSystem.get(config);
    }

    public boolean exists(Path path) {
        try {
            return hdfs.exists(path);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    public FSDataOutputStream create(Path path) {
        try {
            return hdfs.create(path);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public FSDataOutputStream append(Path path) {
        try {
            return hdfs.append(path);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public void createDir(String dir) {
        Path path = new Path(dir);
        try {
            hdfs.mkdirs(path);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void copyFile(String localSrc, String hdfsDst) {
        Path src = new Path(localSrc);
        Path dst = new Path(hdfsDst);
        this.copyFile(src, dst);
    }

    private void copyFile(Path src, Path dst) {
        try {
            hdfs.copyFromLocalFile(src, dst);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void copyFile(boolean delSrc, String localSrc, String hdfsDst) {
        try {
            hdfs.copyFromLocalFile(delSrc, new Path(localSrc), new Path(hdfsDst));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public int createFile(String fileName, String fileContent) {
        try {
            Path dst = new Path(fileName);
            if (!hdfs.exists(dst)) {
                try {
                    byte[] bytes = fileContent.getBytes();
                    FSDataOutputStream output = hdfs.create(dst);
                    output.write(bytes);
                    output.flush();
                    output.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return 0;
                }
                return 1;
            } else {
                FSDataOutputStream output = hdfs.append(dst);
                BufferedWriter out = null;
                try {
                    out = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));
                    out.newLine();
                    out.write(fileContent);
                    out.flush();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return 0;
                } finally {
                    if (out != null) {
                        out.close();
                    }
                }
                return 1;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
        }
    }

    public void createFile(String fileName, byte[] fileContent) {
        try {
            Path dst = new Path(fileName);
            if (hdfs.exists(dst)) {
                return;
            }
            FSDataOutputStream output = hdfs.create(dst, true);
            output.write(fileContent);
            output.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public RemoteIterator<FileStatus> listFiles(Path path) {
        try {
            if (!hdfs.exists(path)) return null;
            return hdfs.listStatusIterator(path);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public boolean dirNotEmpty(Path path) {
        try {
            RemoteIterator<FileStatus> it = listFiles(path);
            return it != null && it.hasNext();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    public void deleteFile(String fileName) {
        deleteFile(new Path(fileName));
    }

    public void deleteFile(Path path) {
        try {
            if (hdfs.exists(path)) {
                hdfs.delete(path, true);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void renameFile(String fileName, String newFileName) {
        try {
            hdfs.rename(new Path(fileName), new Path(newFileName));
        } catch (IllegalArgumentException | IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private byte[] readHDFSFile(String dst) {
        try {
            FileSystem fs = FileSystem.get(config);
            Path path = new Path(dst);
            if (fs.exists(path)) {
                FSDataInputStream is = fs.open(path);
                FileStatus stat = fs.getFileStatus(path);
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                is.readFully(0, buffer);
                is.close();
                fs.close();
                return buffer;
            } else {
                logger.error("the file is not found.");
                return null;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public String[] readText(String dst) {
        try {
            byte[] bytes = readHDFSFile(dst);
            if (bytes != null && bytes.length > 0) {
                String str = new String(readHDFSFile(dst), "UTF-8");
                return str.split("\n");
            } else {
                return new String[0];
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    public FileSystem getHdfs() {
        return hdfs;
    }
}
