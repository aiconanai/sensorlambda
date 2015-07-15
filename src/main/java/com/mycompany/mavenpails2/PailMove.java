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
import backtype.cascading.tap.PailTap;
import backtype.cascading.tap.PailTap.PailTapOptions;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;
import com.mycompany.mavenpails2.Data;
import com.mycompany.mavenpails2.DataUnit;
import com.twitter.maple.tap.StdoutTap;
import elephantdb.DomainSpec;
import elephantdb.jcascalog.EDB;
import elephantdb.partition.HashModScheme;
import elephantdb.persistence.JavaBerkDB;
import java.io.BufferedReader;
import java.io.File;
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
import jcascalog.op.Max;
import jcascalog.op.Avg;
import jcascalog.op.Equals;
import jcascalog.op.Min;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;



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
    
   
    public static Subquery FactsJoin(){
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
        return x;
    }
    
    
    public static Subquery TempBatchView(){
       /*
        Esta Query toma el batch join previamente hecho y calcula la 
        Batch View de los termómetros
        */
        Object source = Api.hfsSeqfile("/tmp/joins");
        Subquery y = new Subquery("?id", "?value","?tipo","?time")
                .predicate(source, "?id", "?value","?tipo", "?time")
                .predicate(new Equals(), "?tipo", "Termometro");
        Subquery z = new Subquery("?id","?twenty","?avg")
                .predicate(y, "?id", "?value", "?tipo", "?time")
                .predicate(new ToTwentyBucket(), "?time").out("?twenty")
                .predicate(new Avg(), "?value").out("?avg");
                
        return z;       
    } 
    
    public static Subquery AnemBatchView1(){
        Object source = Api.hfsSeqfile("/tmp/joins");
        Subquery y = new Subquery("?id", "?value","?tipo","?time")
                .predicate(source, "?id", "?value","?tipo", "?time")
                .predicate(new Equals(), "?tipo", "Anemometro");
        Subquery z = new Subquery("?id","?twenty","?avg")
                .predicate(y, "?id", "?value", "?tipo", "?time")
                .predicate(new ToTwentyBucket(), "?time").out("?twenty")
                .predicate(new Avg(), "?value").out("?avg");
        return z;
    }
    
    public static Subquery AnemBatchView2(){
        Object source = Api.hfsSeqfile("/tmp/joins");
        Subquery y = new Subquery("?id", "?value","?tipo","?time")
                .predicate(source, "?id", "?value","?tipo", "?time")
                .predicate(new Equals(), "?tipo", "Anemometro");
        Subquery z = new Subquery("?id","?ten","?max")
                .predicate(y, "?id", "?value", "?tipo", "?time")
                .predicate(new ToTenBucket(), "?time").out("?ten")
                .predicate(new Max(), "?value").out("?max");
        return z;
    }
    
    public static Subquery AccelBatchView1(){
        Object source = Api.hfsSeqfile("/tmp/joins");
        Subquery y = new Subquery("?id", "?value","?tipo","?time")
                .predicate(source, "?id", "?value","?tipo", "?time")
                .predicate(new Equals(), "?tipo", "Acelerometro");
        Subquery z = new Subquery("?id","?twenty","?absmax")
                .predicate(y, "?id", "?value", "?tipo", "?time")
                .predicate(new ToTwentyBucket(), "?time").out("?twenty")
                .predicate(new AbsMax(), "?value").out("?absmax");
        return z;
    }
    
    
    
    public static void readPail() throws IOException{
        Pail<Data> datapail = new Pail<Data>("/tmp/masterData");
        for (Data d: datapail){
            System.out.println(d.dataunit + " -> "+ d.pedigree);
        }
    }
    
    public static void deleteFolder(File folder) {
    File[] files = folder.listFiles();
    if(files!=null) { //some JVMs return null for empty dirs
        for(File f: files) {
            if(f.isDirectory()) {
                deleteFolder(f);
            } else {
                f.delete();
            }
        }
    }
    folder.delete();
    }
    
    public static void prepWork(){
        Object source = Api.hfsSeqfile("/tmp/joins");
        //Api.execute(new StdoutTap(), FactsJoin());
        Api.execute(source, FactsJoin());
    }
    // A PARTIR DE AQUÍ, SERVING LAYER
    
    public static void accelElephantDB(Subquery accel){
        Subquery toEdb = 
                new Subquery("?key", "?value")
                .predicate(accel, "?id", "?bucket", "?absmax")
                .predicate(new ToIdBucketedKey(), "?id", "?bucket").out("?key")
                .predicate(new ToSerializedInt(), "?absmax").out("?value");
        
        DomainSpec spec = new DomainSpec(new JavaBerkDB(),
                                         new IdOnlyScheme(), 32);
        
        Object tap = EDB.makeKeyValTap("/tmp/outputs/acelerometros", spec);
        Api.execute(new StdoutTap(), toEdb);
    }
   
    public static void main(String args[]) throws Exception {
        //Los primeros pasos son establecer la configuración de Hadoop. 
        setApplicationConf();
        LocalFileSystem fs = FileSystem.getLocal(new Configuration());
        /* Luego creamos Los dos pails necesarios para alojar el masterdataset
        NewDataPail contendrá los registros nuevos y éstos se añadirán al 
        MasterPail, que es el master dataset.
        */
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
       
        /*
        La siguiente rutina toma un archivo de texto de la carpeta resources
        y la desmembra para insertar sus datos en el newDataPail.
        */
        
      /* Pail.TypedRecordOutputStream out = newDataPail.openWrite();
       PailMove c = new PailMove();
       Class cls = c.getClass(); 
       File file = new File(cls.getClassLoader().getResource("dataset.txt").getFile());
 
	try (Scanner scanner = new Scanner(file)) {
 
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			//result.append(line).append("\n");                       
                        StringTokenizer tkn = new StringTokenizer(line);
                        String sid = tkn.nextToken();
                        String stime = tkn.nextToken();
                        String stipo = tkn.nextToken();
                        String seje1 = tkn.nextToken();
                        String seje2 = tkn.nextToken();
                        String selev = tkn.nextToken();
                        String sx = tkn.nextToken();
                        String sy = tkn.nextToken();
                        String sz = tkn.nextToken();
                        String svalue = tkn.nextToken();
                        if(!tkn.hasMoreTokens()){
                            long id = Long.parseLong(sid);
                            int time = Integer.parseInt(stime);
                            int tipo = Integer.parseInt(stipo);
                            int eje1 = Integer.parseInt(seje1);
                            int eje2 = Integer.parseInt(seje2);
                            int elev = Integer.parseInt(selev);
                            int posx = Integer.parseInt(sx);
                            int posy = Integer.parseInt(sy);
                            int posz = Integer.parseInt(sz);
                            long value = Long.parseLong(svalue);
                                              
                            out.writeObject(GenerateData.setValue(id, time, value));
                            out.writeObject(GenerateData.setTipo(id, time, tipo));
                            out.writeObject(GenerateData.setPos(id, time, eje1, eje2, elev, posx,
                                                                posy, posz));
                             
                        }                    
		}
                out.close();
		scanner.close();
	} catch (IOException e) {
		e.printStackTrace();
	} */
        // shred();
        
        
        /* Ingest es una serie de pasos para finalmente insertar el newData en el
        master dataset, evitando duplicancia de datos y corrupción.
        */
        ingest(masterPail, newDataPail);
        // Prepwork crea un join de datos con el fin de cáclular las Batch Views
        prepWork();
        //Finalmente se calculan las Batch Views.
        
        Object source = Api.hfsSeqfile("/tmp/accel");
        Api.execute(new StdoutTap(), TempBatchView());
        Api.execute(new StdoutTap(), AnemBatchView1());
        Api.execute(new StdoutTap(), AnemBatchView2());
        Api.execute(new StdoutTap(), AccelBatchView1());
        
        accelElephantDB(AccelBatchView1());
        /* Elminiamos el directorio temporal joins
           (Debe haber una mejor forma de hacer esto)
        */
        File index = new File("/tmp/joins");
        deleteFolder(index);
        //readPail();

    }
    
}
