/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ServerCommandLine;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

@InterfaceAudience.Private
public class HMasterCommandLine extends ServerCommandLine {
    private static final Logger LOG = LoggerFactory.getLogger(HMasterCommandLine.class);

    private static final String USAGE =
            "Usage: Master [opts] start|stop|clear\n" +
                    " start  Start Master. If local mode, start Master and RegionServer in same JVM\n" +
                    " stop   Start cluster shutdown; Master signals RegionServer shutdown\n" +
                    " clear  Delete the master znode in ZooKeeper after a master crashes\n " +
                    " where [opts] are:\n" +
                    "   --minRegionServers=<servers>   Minimum RegionServers needed to host user tables.\n" +
                    "   --localRegionServers=<servers> " +
                    "RegionServers to start in master process when in standalone mode.\n" +
                    "   --masters=<servers>            Masters to start in this process.\n" +
                    "   --backup                       Master should start in backup mode";

    private final Class<? extends HMaster> masterClass;

    public HMasterCommandLine(Class<? extends HMaster> masterClass) {
        this.masterClass = masterClass;
    }

    @Override
    protected String getUsage() {
        return USAGE;
    }

    /**
     * 根据传入的参数进行判断
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String args[]) throws Exception {
        Options opt = new Options();
        opt.addOption("localRegionServers", true,
                "RegionServers to start in master process when running standalone");
        opt.addOption("masters", true, "Masters to start in this process");
        opt.addOption("minRegionServers", true, "Minimum RegionServers needed to host user tables");
        opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");
        //执行的命令
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opt, args);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            usage(null);
            return 1;
        }


        if (cmd.hasOption("minRegionServers")) {
            String val = cmd.getOptionValue("minRegionServers");
            getConf().setInt("hbase.regions.server.count.min",
                    Integer.parseInt(val));
            LOG.debug("minRegionServers set to " + val);
        }

        // minRegionServers used to be minServers.  Support it too.
        if (cmd.hasOption("minServers")) {
            String val = cmd.getOptionValue("minServers");
            getConf().setInt("hbase.regions.server.count.min", Integer.parseInt(val));
            LOG.debug("minServers set to " + val);
        }

        // check if we are the backup master - override the conf if so
        if (cmd.hasOption("backup")) {
            getConf().setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
        }

        // How many regionservers to startup in this process (we run regionservers in same process as
        // master when we are in local/standalone mode. Useful testing)
        if (cmd.hasOption("localRegionServers")) {
            String val = cmd.getOptionValue("localRegionServers");
            getConf().setInt("hbase.regionservers", Integer.parseInt(val));
            LOG.debug("localRegionServers set to " + val);
        }
        // How many masters to startup inside this process; useful testing
        if (cmd.hasOption("masters")) {
            String val = cmd.getOptionValue("masters");
            getConf().setInt("hbase.masters", Integer.parseInt(val));
            LOG.debug("masters set to " + val);
        }

        @SuppressWarnings("unchecked")
        List<String> remainingArgs = cmd.getArgList();
        if (remainingArgs.size() != 1) {
            usage(null);
            return 1;
        }

        String command = remainingArgs.get(0);
        //通过传入参数执行不同的启动命令
        if ("start".equals(command)) {  //调用启动命令
            return startMaster();
        } else if ("stop".equals(command)) {
            return stopMaster();
        } else if ("clear".equals(command)) {
            return (ZNodeClearer.clear(getConf()) ? 0 : 1);
        } else {
            usage("Invalid command: " + command);
            return 1;
        }
    }

    /**
     * 通过传入参数执行启动
     *
     * @return
     */
    private int startMaster() {
        LOG.info(HMasterCommandLine.class + "：传入start命令启动:startMaster()");
        Configuration conf = getConf();
        //初始化配置文件
        TraceUtil.initTracer(conf);

        try {
            // If 'local', defer to LocalHBaseCluster instance.  Starts master
            // and regionserver both in the one JVM.
            //如果是本运行，master和regionserver都运行一个jvm实例中
            if (LocalHBaseCluster.isLocal(conf)) {
                LOG.info(HMasterCommandLine.class + "：hbase为local模式运行");
                DefaultMetricsSystem.setMiniClusterMode(true);
                //启动一个小型的zk
                final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster(conf);
                LOG.info(HMasterCommandLine.class + "获得zk路径{}" + HConstants.ZOOKEEPER_DATA_DIR);
                //hbase.zookeeper.property.dataDir
                File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));

                // find out the default client port
                int zkClientPort = 0;

                // If the zookeeper client port is specified in server quorum, use it.
                LOG.info(HMasterCommandLine.class + "----HConstants.ZOOKEEPER_QUORUM：" + HConstants.ZOOKEEPER_QUORUM);
                //hbase.zookeeper.quorum
                String zkserver = conf.get(HConstants.ZOOKEEPER_QUORUM);
                //此处解析输入zk地址，并且通过逗号分隔
                if (zkserver != null) {
                    String[] zkservers = zkserver.split(",");
                    LOG.info(HMasterCommandLine.class + "--------zkservers：");
                    System.out.println("------注释-------");
                    Arrays.asList(zkservers).forEach(item -> System.out.println(zkservers));
                    System.out.println("-------注释------");
                    if (zkservers.length > 1) {
                        // In local mode deployment, we have the master + a region server and zookeeper server
                        // started in the same process.
                        // Therefore, we only support one zookeeper server.
                        //本地模式只支持一个zk
                        String errorMsg = "Could not start ZK with " + zkservers.length +
                                " ZK servers in local mode deployment. Aborting as clients (e.g. shell) will not "
                                + "be able to find this ZK quorum.";
                        System.err.println(errorMsg);
                        throw new IOException(errorMsg);
                    }
                    //ip:端口
                    String[] parts = zkservers[0].split(":");
                    //获得zk的端口
                    if (parts.length == 2) {
                        // the second part is the client port
                        zkClientPort = Integer.parseInt(parts[1]);
                    }
                }
                // If the client port could not be find in server quorum conf, try another conf
                //如果找不到zk端口，设置默认
                if (zkClientPort == 0) {
                    //hbase.zookeeper.property.+ clientPort
                    zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
                    // The client port has to be set by now; if not, throw exception.
                    //如果此处还没拿到zk端口，将抛出没有配置zk端口异常
                    if (zkClientPort == 0) {
                        throw new IOException("No config value for " + HConstants.ZOOKEEPER_CLIENT_PORT);
                    }
                }
                //默认为0
                zooKeeperCluster.setDefaultClientPort(zkClientPort);
                // set the ZK tick time if specified
                //hbase.zookeeper.property.
                LOG.info(HMasterCommandLine.class + "———————HConstants.ZOOKEEPER_TICK_TIME" + HConstants.ZOOKEEPER_TICK_TIME);
                int zkTickTime = conf.getInt(HConstants.ZOOKEEPER_TICK_TIME, 0);
                LOG.info(HMasterCommandLine.class + "———————zkTickTime" + String.valueOf(zkTickTime));
                if (zkTickTime > 0) {
                    zooKeeperCluster.setTickTime(zkTickTime);
                }

                // login the zookeeper server principal (if using security)
                //启动zk用户认证（未启用）
                ZKUtil.loginServer(conf, HConstants.ZK_SERVER_KEYTAB_FILE,
                        HConstants.ZK_SERVER_KERBEROS_PRINCIPAL, null);

                int localZKClusterSessionTimeout =
                        conf.getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", 10 * 1000);
                //设置zk的默认超时时间
                LOG.info(HMasterCommandLine.class+"-----zk超时时间"+String.valueOf(localZKClusterSessionTimeout));
                conf.setInt(HConstants.ZK_SESSION_TIMEOUT, localZKClusterSessionTimeout);
                LOG.info("启动Starting a zookeeper cluster");
                int clientPort = zooKeeperCluster.startup(zkDataPath);
                if (clientPort != zkClientPort) {
                    String errorMsg = "Could not start ZK at requested port of " +
                            zkClientPort + ".  ZK was started at port: " + clientPort +
                            ".  Aborting as clients (e.g. shell) will not be able to find " +
                            "this ZK quorum.";
                    System.err.println(errorMsg);
                    throw new IOException(errorMsg);
                }
                conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));

                // Need to have the zk cluster shutdown when master is shutdown.
                //当主机服务器关闭时候，这时候需要关闭zk集群
                // Run a subclass that does the zk cluster shutdown on its way out.
                //运行一个关闭zk集群的子类
                int mastersCount = conf.getInt("hbase.masters", 1);
                int regionServersCount = conf.getInt("hbase.regionservers", 1);
                // Set start timeout to 5 minutes for cmd line start operations
                conf.setIfUnset("hbase.master.start.timeout.localHBaseCluster", "300000");
                LOG.info(HMasterCommandLine.class+"-----+hbase.master.start.timeout.localHBaseCluster:300000");
                LOG.info("Starting up instance of localHBaseCluster; master=" + mastersCount +
                        ", regionserversCount=" + regionServersCount);
                //创建一个本地集群对象
                LOG.info(HMasterCommandLine.class+"--创建一个本地集群对象--new LocalHBaseCluster()");
                LocalHBaseCluster cluster = new LocalHBaseCluster(conf, mastersCount, regionServersCount,
                        LocalHMaster.class, HRegionServer.class);
                ((LocalHMaster) cluster.getMaster(0)).setZKCluster(zooKeeperCluster);
                cluster.startup();
                //等待
                waitOnMasterThreads(cluster);
            } else {
                logProcessInfo(getConf());
                ////生成Hmaster的实例,调用HMaster(finalConfiguration conf)方法
                HMaster master = HMaster.constructMaster(masterClass, conf);
                if (master.isStopped()) {
                    LOG.info("Won't bring the Master up as a shutdown is requested");
                    return 1;
                }
                master.start();
                master.join();
                if (master.isAborted())
                    throw new RuntimeException("HMaster Aborted");
            }
        } catch (Throwable t) {
            LOG.error("Master exiting", t);
            return 1;
        }
        return 0;
    }

    @SuppressWarnings("resource")
    private int stopMaster() {
        Configuration conf = getConf();
        // Don't try more than once
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            try (Admin admin = connection.getAdmin()) {
                admin.shutdown();
            } catch (Throwable t) {
                LOG.error("Failed to stop master", t);
                return 1;
            }
        } catch (MasterNotRunningException e) {
            LOG.error("Master not running");
            return 1;
        } catch (ZooKeeperConnectionException e) {
            LOG.error("ZooKeeper not available");
            return 1;
        } catch (IOException e) {
            LOG.error("Got IOException: " + e.getMessage(), e);
            return 1;
        }
        return 0;
    }

    private void waitOnMasterThreads(LocalHBaseCluster cluster) throws InterruptedException {
        List<JVMClusterUtil.MasterThread> masters = cluster.getMasters();
        List<JVMClusterUtil.RegionServerThread> regionservers = cluster.getRegionServers();

        if (masters != null) {
            for (JVMClusterUtil.MasterThread t : masters) {
                t.join();
                if (t.getMaster().isAborted()) {
                    closeAllRegionServerThreads(regionservers);
                    throw new RuntimeException("HMaster Aborted");
                }
            }
        }
    }

    private static void closeAllRegionServerThreads(
            List<JVMClusterUtil.RegionServerThread> regionservers) {
        for (JVMClusterUtil.RegionServerThread t : regionservers) {
            t.getRegionServer().stop("HMaster Aborted; Bringing down regions servers");
        }
    }

    /*
     * Version of master that will shutdown the passed zk cluster on its way out.
     * 此处使用一个一个内部类
     */

    public static class LocalHMaster extends HMaster {

        private MiniZooKeeperCluster zkcluster = null;

        public LocalHMaster(Configuration conf)
                throws IOException, KeeperException, InterruptedException {
            //加载父类的方法
            super(conf);
        }

        /**
         * 内部类
         */
        @Override
        public void run() {
            //此处调用父类方法
            super.run();
            if (this.zkcluster != null) {
                try {
                    this.zkcluster.shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        void setZKCluster(final MiniZooKeeperCluster zkcluster) {
            this.zkcluster = zkcluster;
        }
    }
}
