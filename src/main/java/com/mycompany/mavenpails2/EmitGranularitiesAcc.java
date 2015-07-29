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
public class EmitGranularitiesAcc extends CascalogFunction{
    @Override
    public void operate(FlowProcess process, FunctionCall call){
        int twentyBucket = call.getArguments().getInteger(0);
        int hourBucket = twentyBucket / 3;
        int dayBucket = hourBucket / 24;
        int weekBucket = dayBucket / 7;
        int monthBucket = weekBucket / 28;
        
        call.getOutputCollector().add(new Tuple("twenty", twentyBucket));
        call.getOutputCollector().add(new Tuple("h", hourBucket));
        call.getOutputCollector().add(new Tuple("d", dayBucket));
        call.getOutputCollector().add(new Tuple("w", weekBucket));
        call.getOutputCollector().add(new Tuple("m", monthBucket));
    }
    
}
