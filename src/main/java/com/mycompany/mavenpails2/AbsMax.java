/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.tuple.Tuple;
import cascalog.CascalogAggregator;

/**
 *
 * @author francisco
 */
public class AbsMax extends CascalogAggregator {
    @Override
    public void start(FlowProcess process, AggregatorCall call){
        call.setContext(0);
      
    }
    
    @Override
    public void aggregate(FlowProcess process, AggregatorCall call){
        String c = call.getContext().toString();
        double context = Double.parseDouble(c);
        //Si el contexto es negativo
        if(context < 0){
          double abscontext = context * -1;
           //Si  también el argumento es negativo
            if(call.getArguments().getDouble(0)<0){
                double absarg = call.getArguments().getDouble(0) * -1;
                //Si el argumento absoluto es mayor que el contexto absoluto
                if(absarg > abscontext){
                    call.setContext(call.getArguments().getDouble(0));
                }
            }
            // Si el argumento es positivo (y el contexto negativo)
            else{
                if (call.getArguments().getDouble(0) > abscontext){
                    call.setContext(call.getArguments().getDouble(0));
                }
            }
        }
        //Si el contexto es positivo
        else{
            // Si el argumento es negativo
            if(call.getArguments().getDouble(0)<0){
                double absarg = call.getArguments().getDouble(0) * -1;
                //Si el argumento absoluto es mayor que el contexto
                if(absarg > context){
                    call.setContext(call.getArguments().getDouble(0));
                }
            }
            //Si el argumento es positivo
            else{
                if (call.getArguments().getDouble(0)>context){
                    call.setContext(call.getArguments().getDouble(0));
                }
            }
        }
       
        
        
    }
    
    @Override
    public void complete(FlowProcess process, AggregatorCall call){
        double AbsMax = (Double) call.getContext();
        call.getOutputCollector().add(new Tuple(AbsMax));
    }
}
