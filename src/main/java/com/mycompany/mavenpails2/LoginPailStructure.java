/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import com.backtype.hadoop.pail.PailStructure;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author fedora
 */
public class LoginPailStructure implements PailStructure<Login> {
   
    @Override
    public Class getType() {
        return Login.class;
    }
    
    @Override
    public byte[] serialize(Login login){
        ByteArrayOutputStream byteout = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteout);
        byte[] userBytes = login.userName.getBytes();
        try {
            dataOut.writeInt(userBytes.length);
            dataOut.write(userBytes);
            dataOut.writeLong(login.loginUnixTime);
            dataOut.close();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        return byteout.toByteArray();       
    }

    @Override
    public boolean isValidTarget(String... dirs) {
        return true;
    }

    @Override
    public Login deserialize(byte[] serialized) {
       DataInputStream dataIn =
               new DataInputStream(new ByteArrayInputStream(serialized));
       try{
           byte[] userBytes = new byte[dataIn.readInt()];
           dataIn.read(userBytes);
           return new Login(new String(userBytes), dataIn.readLong());
       } catch(IOException e) {
           throw new RuntimeException(e);
       }
    }

    @Override
    public List<String> getTarget(Login object) {
       return Collections.EMPTY_LIST;
    }
    
}
