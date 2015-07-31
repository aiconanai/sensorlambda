/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 *
 * @author francisco
 */
public class CassandraDatastax {
    private Cluster cluster;
    private Session session;
    
    public Session getSession(){
        return this.session;
    }
    
    public void connect(String node){
        cluster = Cluster.builder()
         .addContactPoint(node)
         .build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", 
        metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
             System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
             host.getDatacenter(), host.getAddress(), host.getRack());
         }
        session = cluster.connect();
    }
    
    public void createSchema(){
     
      session.execute("CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH replication " + 
            "= {'class':'SimpleStrategy', 'replication_factor':3};");
      session.execute(
            "CREATE TABLE IF NOT EXISTS mykeyspace.sensor (" +
                  "id int," + 
                  "bucket text," + 
                  "time int," + 
                  "value int," + 
                  "PRIMARY KEY(id, bucket)" +  
                  ");");
    }
    
    public void loadData(){
        session.execute(
            "INSERT INTO mykeyspace.sensor (id, bucket, time, value) " +
            "VALUES (" +
                "1," +
                "'twenty'," +
                "1183392," +
                "-5921)" +
                ";");
    }
    
    public void close() {
        cluster.close();
    }

}
