/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;



import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascalog.ops.IdentityBuffer;
import cascalog.ops.RandLong;
import clojure.lang.Keyword;
import clojure.lang.PersistentStructMap;
import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.mycompany.mavenpails2.Data;
import com.mycompany.mavenpails2.DataUnit;
import com.twitter.maple.tap.StdoutTap;
import elephantdb.DomainSpec;
import elephantdb.jcascalog.EDB;
import elephantdb.partition.HashModScheme;
import elephantdb.persistence.JavaBerkDB;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.LT;
import jcascalog.op.GT;
import jcascalog.op.Sum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import jcascalog.op.Count;

/**
 *
 * @author fedora
 */
public class PailMove {
    
    public static final String TEMP_DIR = "/tmp/swa";
    public static final String NEW_DATA_LOCATION = "/tmp/newData";
    public static final String MASTER_DATA_LOCATION = "/tmp/masterData";
    public static final String SNAPSHOT_LOCATION = "/tmp/swa/newDataSnapshot";
    public static final String SHREDDED_DATA_LOCATION = "/tmp/swa/shredded";
    
    
    public static void mergeData(String masterDir, String updateDir) throws IOException {
        Pail target = new Pail(masterDir);
        Pail source = new Pail(updateDir);
        target.absorb(source);
        target.consolidate();
    }
    
    
    public static void setApplicationConf() throws IOException {
        Map conf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization,org.apache.hadoop.io.serializer.WritableSerialization";
        conf.put("io.serializations", sers);
        Api.setApplicationConf(conf);
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(TEMP_DIR), true);
        fs.mkdirs(new Path(TEMP_DIR));
        /* Configuration conf2 = new Configuration();
        FileSystem fs = FileSystem.get(conf2);
        fs.delete(new Path(TEMP_DIR), true);
        fs.mkdirs(new Path(TEMP_DIR)); */
    }
    
    
    
    public static void ingest(Pail masterPail, Pail newDataPail) throws IOException {
        
     
        Pail snapshotPail = newDataPail.snapshot(SNAPSHOT_LOCATION);
        //shred();
        appendNewData(masterPail, snapshotPail);
        //consolidateAndAbsord(masterPail, new Pail(SHREDDED_DATA_LOCATION));
        newDataPail.deleteSnapshot(snapshotPail); 
    }
    
    
    /* private static void consolidateAndAbsord(Pail masterPail, Pail shreddedPail) throws IOException {
        shreddedPail.consolidate();
        masterPail.absorb(shreddedPail);
    } */

    
    public static PailTap attributeTap(String path, final Data._Fields... fields) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.attrs = new List[] {
        new ArrayList<String>() {{
            for (Data._Fields field: fields) {
                 add("" + field.getThriftFieldId());
            }
        }}
        };
        opts.spec = new PailSpec((PailStructure) new SplitDataPailStructure());
        return new PailTap(path, opts);
    }
    
    public static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec((PailStructure) new SplitDataPailStructure());
        return new PailTap(path, opts);
    }
    
    public static PailTap deserializeDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(new DataPailStructure());
        return new PailTap(path, opts);
    }
    
    public static Pail shred() throws IOException {
        PailTap source = deserializeDataTap(SNAPSHOT_LOCATION);
        PailTap sink = splitDataTap(SHREDDED_DATA_LOCATION);
        Subquery reduced = new Subquery("?rand", "?data")
        .predicate(source, "_", "?data-in")
        .predicate(new RandLong())
        .out("?rand")
        .predicate(new IdentityBuffer(), "?data-in")
        .out("?data");
        Api.execute(sink, new Subquery("?data").predicate(reduced, "_", "?data"));
        
        Pail shreddedPail = new Pail(SHREDDED_DATA_LOCATION);
        shreddedPail.consolidate();
        return shreddedPail;
        
    }
    
    public static void appendNewData(Pail masterPail, Pail snapshotPail) throws IOException{
        Pail shreddedPail = shred();
        masterPail.absorb(shreddedPail);
    }
    
    
    
    public static Subquery getValue(){
       
        PailTap masterData = splitDataTap("/tmp/masterData");
        Subquery a = new Subquery("?id", "?time","?value")
                .predicate(masterData, "_","?data")
                .predicate(new ExtractValueFields(), "?data")
                .out("?id","?value","?time");    
        Subquery b = new Subquery("?id", "?tipo", "?time")
                .predicate(masterData, "_","?data")
                .predicate(new ExtractTypeField(), "?data")
                .out("?id","?tipo", "?time");
        Subquery x = new Subquery("?id", "?value","?tipo","?time")
                .predicate(a, "?id", "?time", "?value")
                .predicate(b, "?id", "?tipo", "?time");
        Subquery y = new Subquery("?id", "?hour-bucket")
                .predicate(x, "?id", "?value","?tipo", "?time")
                .predicate(new ToHourBucket(), "?time").out("?hour-bucket")
                ;
                
        return y;       
    } 
    
    
    
    public static void readPail() throws IOException{
        Pail<Data> datapail = new Pail<Data>("/tmp/masterData");
        for (Data d: datapail){
            System.out.println(d.dataunit + " -> "+ d.pedigree);
        }
    }
    
    // A PARTIR DE AQUÍ, SERVING LAYER
    
  
   
    public static void main(String args[]) throws Exception {
        setApplicationConf();
        LocalFileSystem fs = FileSystem.getLocal(new Configuration());
        Pail newDataPail;
        Pail masterPail;
        Path fils = new Path(NEW_DATA_LOCATION);
        if (!fs.exists(fils)) {
            newDataPail = Pail.create(FileSystem.get(new Configuration()), 
                                    NEW_DATA_LOCATION, new DataPailStructure());
        } else {
            newDataPail = new Pail<Data>(NEW_DATA_LOCATION);
        }
        if (!fs.exists(new Path(MASTER_DATA_LOCATION))) {
            masterPail = Pail.create(FileSystem.getLocal(new Configuration()), 
                            MASTER_DATA_LOCATION, new SplitDataPailStructure());
        } else {
            masterPail = new Pail<Data>(MASTER_DATA_LOCATION);
        }
        
       /* try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line;
            while ((line = br.readLine()) != null) {
                
            }
        } */

        
       /* Pail.TypedRecordOutputStream out = newDataPail.openWrite();
   
        out.writeObject(GenerateData.setValue(1, 1434153600, 5123));
        out.writeObject(GenerateData.setTipo(1, 1434153600, 1));
        out.writeObject(GenerateData.setPos(1, 1434153600, 10, 5, 1, 2, 0, 10));
        
        out.writeObject(GenerateData.setValue(2, 1434153600, 25));
        out.writeObject(GenerateData.setTipo(2, 1434153600, 3));
        out.writeObject(GenerateData.setPos(2, 1434153600, 10, 7, 2, 2, 0, 1));
        
        out.writeObject(GenerateData.setValue(2, 1434153660, 22));
        out.writeObject(GenerateData.setTipo(2, 1434153660, 3));
        out.writeObject(GenerateData.setPos(2, 1434153660, 10, 7, 2, 2, 0, 1));
        
        out.writeObject(GenerateData.setValue(2, 1434153660, 22));
        out.writeObject(GenerateData.setTipo(2, 1434153660, 3));
        out.writeObject(GenerateData.setPos(2, 1434153660, 10, 7, 2, 2, 0, 1));
        
        out.writeObject(GenerateData.setValue(2, 1434153720, 22));
        out.writeObject(GenerateData.setTipo(2, 1434153720, 3));
        out.writeObject(GenerateData.setPos(2, 1434153720, 10, 7, 2, 2, 0, 1));
        
        out.writeObject(GenerateData.setValue(3, 1434153720, 34));
        out.writeObject(GenerateData.setTipo(3, 1434153720, 2));
        out.writeObject(GenerateData.setPos(3, 1434153720, 5, 10, 1, 2, 10, 2));
        
        out.writeObject(GenerateData.setValue(1, 1434153660, 1242));
        out.writeObject(GenerateData.setTipo(1, 1434153660, 1));
        out.writeObject(GenerateData.setPos(1, 1434153660, 10, 5, 1, 2, 0, 10));
        
        out.close(); */
        // shred();
        
        ingest(masterPail, newDataPail);
        Api.execute(new StdoutTap(), getValue());
        //readPail();
        //normalizeURLs();
// normalizeUserIds();
       // deduplicatePageviews();
        //pageviewBatchView();
        
        //bouncesView();
    }
    
}
