/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 *
 * @author francisco
 */
public class StreamProcessing {
    
    public void StormModelAcc() throws InvalidTopologyException, AlreadyAliveException{
        TopologyBuilder builder = new TopologyBuilder();
        //Creamos el spout.
        builder.setSpout("streamer",new originSpout(), 8);
        /*
        A partir de aquí, creamos los bolts necesarios para procesar el stream.
        El bolt splitter que dividirá el stream en las tuplas (id, tipo, valor, time)
        El bolt filtro que filtrará solo los tipo = acelerometro
        El bolt que juntará 20 minutos de stream y calculará el absmax.
        */
        builder.setBolt("Splitter", new BoltSplitter(), 12)
                .shuffleGrouping("originSpout");
        builder.setBolt("AccFilter", new BoltFilter(), 12)
                .shuffleGrouping("Splitter");
        builder.setBolt("AbsMaxTwenty", new BoltAbsMax(), 12);
        
        Config conf = new Config();
        conf.setNumWorkers(4);
        StormSubmitter.submitTopology("AccRTView", conf, builder.createTopology());
        conf.setMaxSpoutPending(1000);
    }
}
