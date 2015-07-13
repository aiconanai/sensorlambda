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

/**
 *
 * @author francisco
 */
class ToTenBucket extends CascalogFunction {
    private static final int TWENTY_IN_SECS = 60 * 10;
    
    @Override
    public void operate(FlowProcess process, FunctionCall call){
        int timestamp = call.getArguments().getInteger(0);
        int hourBucket = timestamp / TWENTY_IN_SECS;
        call.getOutputCollector().add(new Tuple(hourBucket));
    }
  
    
}
