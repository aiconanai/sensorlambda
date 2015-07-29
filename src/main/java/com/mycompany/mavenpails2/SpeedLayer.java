/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;


import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 *
 * @author francisco
 */
public class SpeedLayer {
    
    public void CassandraPrepwork(){
    Cluster cluster = HFactory.getOrCreateCluster("sensorcluster", "127.0.0.1");    
    Keyspace keyspace = HFactory.createKeyspace("sensornetwork", cluster);
    
    ColumnFamilyTemplate<String, Integer> template = 
            new ThriftColumnFamilyTemplate<> (keyspace,
                                              "sensornetwork",
                                              StringSerializer.get(),
                                              IntegerSerializer.get());
    
   
    SliceQuery<String, Integer, Integer> slice =
            HFactory.createSliceQuery(keyspace, 
                    StringSerializer.get(),
                    IntegerSerializer.get(), 
                    IntegerSerializer.get());
    
    slice.setColumnFamily("sensornetwork");
    slice.setKey("1/1183392");
    
    ColumnSliceIterator<String, Integer, Integer> it =
            new ColumnSliceIterator<>(slice,
            20, 55, false);
    
    int total = 0;
        while(it.hasNext()){
            total += it.next().getValue();
        }
    
        int currVal;
        HColumn<Integer, Integer> col =
                template.querySingleColumn("1/1183392", 7, IntegerSerializer.get());
        
        if(col == null){
            currVal = 0;
        }
        else currVal = col.getValue();
    
    ColumnFamilyUpdater<String,Integer> updater =
       template.createUpdater("1/1183392");
    updater.setInteger(7, currVal + 25);
    template.update(updater);
    
        
    }   
   
}
