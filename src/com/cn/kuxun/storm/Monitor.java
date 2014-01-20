package com.cn.kuxun.storm;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;
//import org.quartz.CronTrigger;
//import org.quartz.JobDetail;
//import org.quartz.Scheduler;
//import org.quartz.SchedulerFactory;
//import org.quartz.impl.StdSchedulerFactory;
//
//import com.qunar.datateam.storm.db.DBCommunicator;
//import com.qunar.datateam.storm.db.DBConnector;
//import com.qunar.datateam.storm.monitor.service.TimerScheduler;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

//import com.qunar.datateam.storm.monitor.service.UpdateJobNameJob;
//import com.qunar.datateam.storm.util.Constant;
//import com.qunar.datateam.storm.util.DateUtil;

public class Monitor {

        public static Logger LOG = Logger.getLogger(Monitor.class);    
        
        public static TSocket tsocket = null;
        public static TFramedTransport tTransport = null;
        public static TBinaryProtocol tBinaryProtocol = null;
        public static Nimbus.Client client = null;
        
        public String host = "192.168.1.50";      //default nimbus host
        public int port = 6627;                    //default nimbus port
        
        public static CopyOnWriteArraySet<String> jobNames = null;    //job names to monit
        
//        public static DBConnector connector = null;
//        public static Connection con = null;
//        public static DBCommunicator communicator = null;s
        
//        public static TimerScheduler ts = null;
        
        
        public Monitor(){
                new Monitor(host, port);
        }
        
        public Monitor(String nimbusHost, int nimbusPort){
                tsocket = new TSocket(nimbusHost, nimbusPort);
                tTransport = new TFramedTransport(tsocket);
                tBinaryProtocol = new TBinaryProtocol(tTransport);
                client = new Nimbus.Client(tBinaryProtocol);
                try {
                        tTransport.open();
                } catch (TTransportException e) {
                        e.printStackTrace();
                }
                
                jobNames = new CopyOnWriteArraySet<String>();
                
        }
        
        public static String getTopologyId(String name) {
                if (name == null)
                        return null;
        try {
                ClusterSummary summary = client.getClusterInfo();
                
            for(TopologySummary s : summary.get_topologies()) {
                if(s.get_name().equals(name)) {  
                        
                        String id = s.get_id();
//                        LOG.info("Topology " + name + " exists ! " + "id : " + id);
                    return id;
                } 
            }  
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
//        LOG.info("Topology " + name + " not exists ! ");
        return null;
    }
        
        public void close(){
                tTransport.close();
        }
        
        public static ArrayList<String> getTopologyIds(String[] names){
                ArrayList<String> rs = new ArrayList<String>();
                for (String name : names){
                        rs.add(getTopologyId(name));
                }
                return rs;
        }
        
        public static HashMap<String, ArrayList<String>> stasticTopologyInfo(String topologyId){
                if (topologyId == null){
//                        LOG.warn("Topology id is null !");
                        return null;
                }
                
                //key : host_component
                //value : emit or transit value
                HashMap<String, ArrayList<String>> rs = new HashMap<String, ArrayList<String>>();
                
                try{
//                        ClusterSummary clusterSummary = client.getClusterInfo();
//                        StormTopology stormTopology = client.getTopology(topologyId);
                        TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
                        
                        List<ExecutorSummary> executorSummaries = topologyInfo.get_executors();
//                        List<TopologySummary> topologies = clusterSummary.get_topologies();
                        
                        for(ExecutorSummary executorSummary : executorSummaries) {
                                String id = executorSummary.get_component_id();
                                
//                                ExecutorInfo executorInfo = executorSummary.get_executor_info();
                                
                                ExecutorStats executorStats = executorSummary.get_stats();
                                
                                String host = executorSummary.get_host();
                                String component = id;
                                String host_componet = String.format("%s\t%s", host,component);
                                Map<String, Map<String, Long>> ts = executorStats.get_transferred();
                                
                                //处理 emitted 类型的数据
                                if (executorStats.get_emitted().get("600").size() == 0){
                                        if (!rs.containsKey(host_componet)){
                                                ArrayList<String> tmpArray = new ArrayList<String>();
                                                tmpArray.add("emmitted\t" + 0);
                                                rs.put(host_componet, tmpArray);
                                        }
                                        else {
                                                rs.get(host_componet).add("emitted\t" + 0);
                                        }
                                        
                                } else {
                                        String tmp = executorStats.get_emitted().get("600").get("default") + "";
                                        
                                        if (!rs.containsKey(host_componet)){
                                                ArrayList<String> tmpArray = new ArrayList<String>();
                                                tmpArray.add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
                                                rs.put(host_componet, tmpArray);
                                        } else {
                                                rs.get(host_componet).add("emitted\t" + (tmp.equals("null") ? "0" : tmp));
                                        }
                                        
                                }
                                
                        }
                        
                }catch(TTransportException e){
                        e.printStackTrace();
                } catch (TException e) {
                        e.printStackTrace();
                } catch (NotAliveException e) {
                        e.printStackTrace();
                }
                
                return rs;
        }
        
        
        public static long getEmitByComponentId(HashMap<String, ArrayList<String>> data, String jobName, String jobId, String componentId) {
        	long emitCount = 0;
        	for (Entry<String, ArrayList<String>> en : data.entrySet()){
                String[] tmpKey = en.getKey().split("\t");       //0:host 1:component
                if(tmpKey[1].equals(componentId)) {
                	for (String value : en.getValue()) {
                        String[] tmpValue = value.split("\t");         //0:type 1:value
                        emitCount = emitCount + Long.parseLong(tmpValue[1]);
                	}
                }
        	}
        	return emitCount;
        }
      
        
        public static void main(String[] args) throws InterruptedException, SQLException{
                Monitor monitor = new Monitor();
                String jobId = getTopologyId(args[0]);
//                String jobId = getTopologyId("crawler");
                HashMap<String, ArrayList<String>> jobStatus = stasticTopologyInfo(jobId);
                long emitCount = getEmitByComponentId(jobStatus, args[0], jobId, args[1]);
                System.out.println(emitCount);
        }
        
}
