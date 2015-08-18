/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import backtype.storm.spout.MultiScheme;
import backtype.storm.tuple.Fields;
import java.util.List;

/**
 *
 * @author francisco
 */
class AccelScheme implements MultiScheme {

    public AccelScheme() {
    }

    @Override
    public Iterable<List<Object>> deserialize(byte[] bytes) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Fields getOutputFields() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
