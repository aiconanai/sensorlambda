/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author francisco
 */
class BoltAbsMax extends BaseRichBolt {
    
    List<Tuple> _buffer = new ArrayList<>();
    int _id; //Usaré este entero para saber si la tupla que llega es del mismo id.
    int _absmax;
    OutputCollector _collector;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        
    }
    

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
       _collector = oc;
       
    }

    @Override
    public void execute(Tuple tuple) {
        int value = tuple.getInteger(2);
        int id = tuple.getInteger(0);
        int abs;
        
        if(value < 0){

            _buffer.add(tuple);
        }
        else {
            _buffer.add(tuple);
        }
    }

   
    
}
