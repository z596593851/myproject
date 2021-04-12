package com.hxm.core.log;

import com.hxm.core.utils.KafkaScheduler;
import com.hxm.core.server.OffsetCheckpoint;
import com.hxm.client.common.utils.Time;
import com.hxm.client.common.TopicPartition;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class LogManager {
    private File[] logDirs;
    private int ioThreads;
    private Map<TopicPartition,Log> logs;
    private String RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint";
    private Map<File,OffsetCheckpoint> recoveryPointCheckpoints;
    private Time time=new Time();
    private final KafkaScheduler scheduler;
    private final long InitialTaskDelayMs = 3*1000;
    private final long flushCheckMs;
    private final long flushCheckpointMs;


    public LogManager(File[] logDirs,int ioThreads, KafkaScheduler scheduler,long flushCheckMs, long flushCheckpointMs){
        this.logDirs=logDirs;
        this.ioThreads=ioThreads;
        this.logs=new HashMap<>();
        this.scheduler=scheduler;
        this.flushCheckMs=flushCheckMs;
        this.flushCheckpointMs=flushCheckpointMs;
        createAndValidateLogDirs(logDirs);
        recoveryPointCheckpoints=new HashMap<>();
        for (File dir : logDirs) {
            recoveryPointCheckpoints.put(dir,new OffsetCheckpoint(new File(dir,RecoveryPointCheckpointFile)));
        }
        loadLogs();
    }


    private void createAndValidateLogDirs(File[] logDirs){
        for(File dir:logDirs){
            if(!dir.exists()){
                boolean created=dir.mkdir();
                if(!created){
                    throw new RuntimeException("Failed to create data directory "+dir.getAbsolutePath());
                }
            }
            if(!dir.isDirectory()||!dir.canRead()){
                throw new RuntimeException(dir.getAbsolutePath()+" is not a readable log directory.");
            }
        }
    }

    private void loadLogs(){
        List<ExecutorService> threadPools=new ArrayList<>();
        List<Future<?>> jobs=new ArrayList<>();
        try {
            for (File dir : logDirs) {
                //为每个目录都创建指定线程数的线程池
                ExecutorService pool= Executors.newFixedThreadPool(ioThreads);
                threadPools.add(pool);
                //读取每个log目录下的recoveryPointsCheckpoint文件并生成TopicAndPartition和recoveryPoints的对应关系
                //加载recoveryPoints
                Map<TopicPartition,Long> recoveryPoints =recoveryPointCheckpoints.get(dir).read();
                //为每个log文件夹创建一个runnable任务
                Arrays.stream(Objects.requireNonNull(dir.listFiles())).filter(File::isDirectory).forEach(logDir->{
                    Runnable runnable=()->{
                        //从文件名解析topic名和分区号
                        TopicPartition topicPartition= null;
                        try {
                            topicPartition = Log.parseTopicPartitionName(logDir);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        //获取log对应RecoveryPoint
                        long logRecoveryPoint=recoveryPoints.getOrDefault(topicPartition,0L);
                        //创建log对象
                        Log current=new Log(logDir,logRecoveryPoint,scheduler,time);
                        //将log对象保存到logs集合中，所有分区到log成功加载完
                        Log previous=logs.put(topicPartition,current);
                        if(previous!=null){
                            throw new IllegalArgumentException(
                                    String.format("Duplicate log directories found: %s, %s!", current.getDir().getAbsolutePath(), previous.getDir().getAbsolutePath()));
                        }
                    };
                    jobs.add(pool.submit(runnable));
                });

            }
            //等待jobs中的线程完成
            for (Future job : jobs) {
                job.get();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            threadPools.forEach(ExecutorService::shutdown);
        }
    }

    public void startup() {
        if(scheduler!=null){
            scheduler.schedule("kafka-log-flusher", this::flushDirtyLogs, InitialTaskDelayMs, flushCheckMs, TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-recovery-point-checkpoint", this::checkpointRecoveryPointOffsets, InitialTaskDelayMs,flushCheckpointMs,TimeUnit.MILLISECONDS);
        }

    }

    public Log getLog(){
        return null;
    }
    private void checkpointRecoveryPointOffsets(){
        for (File logDir : logDirs) {
            checkpointLogsInDir(logDir);
        }
    }

    private void checkpointLogsInDir(File dir){
        recoveryPointCheckpoints.get(dir).write(logs);
    }

    private void flushDirtyLogs(){
        for(Map.Entry<TopicPartition,Log> entry:logs.entrySet()){
            Log log=entry.getValue();
            long timeSinceLastFlush = time.milliseconds() - log.getLastflushedTime();
            if(timeSinceLastFlush>=10000){
                log.flush();
            }
        }
    }

    public Log createLog(TopicPartition topicPartition){
        Log log=logs.get(topicPartition);
        if(log!=null){
            return log;
        }
        File dataDir=nextLogDir();
        File dir=new File(dataDir,topicPartition.topic() + "-" + topicPartition.partition());
        dir.mkdir();
        log=new Log(dir,0L,scheduler,time);
        logs.put(topicPartition,log);
        return log;

    }

    private File nextLogDir(){
        if(logDirs.length==1){
            return logDirs[0];
        }else {
            throw new RuntimeException("只有一个logDir");
        }
    }
}
