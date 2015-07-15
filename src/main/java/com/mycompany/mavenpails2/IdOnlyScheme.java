/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import elephantdb.partition.ShardingScheme;
import java.io.UnsupportedEncodingException;

/**
 *
 * @author francisco
 */
public class IdOnlyScheme implements ShardingScheme {
    
    private static int getIdFromSerializedKey(byte[] ser){
        try{
            String key = new String(ser, "UTF-8");
            String aux = key.substring(0, key.lastIndexOf("/"));
            int keyint = Integer.parseInt(aux);
            return keyint;
        } catch(UnsupportedEncodingException e){
            throw new RuntimeException(e);
        }
    }
    @Override
    public int shardIndex(byte[] shardKey, int shardCount){
        Integer id = getIdFromSerializedKey(shardKey);     
        
        return id.hashCode() % shardCount;
    }
}
