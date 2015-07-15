/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author francisco
 */
public class ToIdBucketedKey extends CascalogFunction {
    
    @Override
    public void operate(FlowProcess process, FunctionCall call){
        int id = call.getArguments().getInteger(0);
        int bucket = call.getArguments().getInteger(1);
        
        String KeyStr = id + "/" + bucket;
        try{
            call.getOutputCollector().add(new Tuple(KeyStr.getBytes("UTF-8")));
        } catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
    }
}
