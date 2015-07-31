/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



/**
 *
 * @author francisco
 */
class BoltSplitter extends BaseBasicBolt {

 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("id", "tipo", "valor", "timestamp"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
        int id = tuple.getInteger(0);
        int timestamp = tuple.getInteger(1);
        int valor = tuple.getInteger(9);
        String tipo = tuple.getString(2);
        
        boc.emit(new Values(id, tipo, valor, timestamp));
    }
    
}
