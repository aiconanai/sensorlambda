/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author francisco
 */
class BoltFilter extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector boc) {
       if(tuple.getString(1).equals("Acelerometro")){
            int id = tuple.getInteger(0);
            String tipo = tuple.getString(1);
            int value = tuple.getInteger(2);
            int timestamp = tuple.getInteger(3);
       }
    }

   
    
}
