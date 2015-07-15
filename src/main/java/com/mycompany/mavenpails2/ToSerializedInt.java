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
import java.nio.ByteBuffer;

/**
 *
 * @author francisco
 */
public class ToSerializedInt extends CascalogFunction {
    
    @Override
    public void operate(FlowProcess process, FunctionCall call){
        int val = call.getArguments().getInteger(0);
        
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(val);
        
        call.getOutputCollector().add(new Tuple(buffer.array()));
    }
}
