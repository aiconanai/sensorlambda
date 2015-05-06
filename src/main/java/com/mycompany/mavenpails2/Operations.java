/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import java.io.IOException;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;
import java.util.HashMap;
import java.util.Map;


public class Operations {
    public static void simpleIO() throws IOException{
        Pail pail = Pail.create("/tmp/mypail");
        TypedRecordOutputStream os = pail.openWrite();
        
        os.writeObject(new byte[] {1, 2, 3});
        os.writeObject(new byte[] {1, 2, 3, 4});
        os.writeObject(new byte[] {1, 2, 3, 4, 5});
        
        os.close();
    }
    
    public static void writeLogins() throws IOException {
        Pail<Login> loginPail = Pail.create("/tmp/logins",
                                            new LoginPailStructure());
        TypedRecordOutputStream out = loginPail.openWrite();
        out.writeObject(new Login("alex", 1352679231));
        out.writeObject(new Login("bob", 1352674216));
        out.close();
    }
    
    public static void readLogins() throws IOException{
        Pail<Login> loginPail = new Pail<Login>("/tmp/logins");
        for(Login l: loginPail){
        System.out.println(l.userName + " " + l.loginUnixTime);
    }
    }
    
    public static void partitionData() throws IOException{
        Pail<Login> loginPail = Pail.create("/tmp/partitioned_logins",
                                            new PartitionedLoginPailStructure());
        TypedRecordOutputStream out = loginPail.openWrite();
        out.writeObject(new Login("chris", 1352702020));
        out.writeObject(new Login("david", 1352788472));
        out.close();
    }
    
    public static void createCompressedPail() throws IOException{
        Map<String, Object> options = new HashMap<String, Object>();
        options.put(SequenceFileFormat.CODEC_ARG, 
                    SequenceFileFormat.CODEC_ARG_GZIP);
        options.put(SequenceFileFormat.TYPE_ARG,
                    SequenceFileFormat.TYPE_ARG_BLOCK);
        LoginPailStructure struct = new LoginPailStructure();
        
        Pail compressed = Pail.create("/tmp/compressed",
                                      new PailSpec("SequenceFile", options,
                                      struct));
    }
    
    public static void appendData() throws IOException {
        Pail<Login> loginPail = new Pail<Login>("/tmp/logins");
        Pail<Login> updatePail = new Pail<Login>("/tmp/updates");
        loginPail.absorb(updatePail);
    }
    
}
