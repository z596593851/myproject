package com.hxm.core.server;

import com.hxm.client.common.utils.Utils;
import com.hxm.core.log.Log;
import com.hxm.client.common.TopicPartition;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class OffsetCheckpoint {
    private File file;
    private Path path;
    private Path tempPath;
    private int CurrentVersion = 0;
    private Pattern WhiteSpacesPattern = Pattern.compile("\\s+");

    public OffsetCheckpoint(File file){
        this.file=file;
        this.path=file.toPath().toAbsolutePath();
        this.tempPath=Paths.get(path.toString()+".temp");
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Map<TopicPartition, Log> offsets) {
        BufferedWriter writer=null;
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
            writer= new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            writer.write(Integer.toString(CurrentVersion));
            writer.newLine();

            writer.write(String.valueOf(offsets.size()));
            writer.newLine();
            for(Map.Entry<TopicPartition, Log> entry:offsets.entrySet()){
                writer.write(String.format("%s %d %d",entry.getKey().topic(),entry.getKey().partition(),entry.getValue().getRecoveryPoint()));
                writer.newLine();
            }
            writer.flush();
            fileOutputStream.getFD().sync();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            Utils.atomicMoveWithFallback(tempPath,path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<TopicPartition,Long> read() throws Exception{
        Map<TopicPartition,Long> empty=new HashMap<>();
        BufferedReader reader=new BufferedReader(new FileReader(file));;
        String line=null;
        try {
            line = reader.readLine();
            if(line==null){
                return empty;
            }
            int version=Integer.parseInt(line);
            if(version==CurrentVersion){
                line=reader.readLine();
                if(line==null){
                    return empty;
                }
                int expectedSize=Integer.parseInt(line);
                Map<TopicPartition,Long> offsets=new HashMap<>();
                line=reader.readLine();
                while (line!=null){
                    String[] s=WhiteSpacesPattern.split(line);
                    offsets.put(new TopicPartition(s[0],Integer.parseInt(s[1])),Long.parseLong(s[2]));
                    line=reader.readLine();
                }
                if(offsets.size()!=expectedSize){
                    throw new RuntimeException("Expected "+expectedSize+" entries but found only "+offsets.size());
                }
                return offsets;
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            reader.close();
        }
        return null;
    }


}
