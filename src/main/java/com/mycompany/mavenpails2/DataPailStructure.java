/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import java.util.Collections;
import java.util.List;



/**
 *
 * @author fedora
 */
public class DataPailStructure extends ThriftPailStructure<Data> {

    @Override
    public Class getType() {
        return Data.class;
    }
    
    @Override
    protected Data createThriftObject(){
        return new Data();
    }
    
    @Override
    public List<String> getTarget(Data Object) {
        return Collections.EMPTY_LIST;
    }
    
    @Override
    public boolean isValidTarget(String... dirs){
        return true;
    }

    
}
