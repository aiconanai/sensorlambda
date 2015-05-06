/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import java.util.HashMap;

import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;



/**
 *
 * @author fedora
 */
import java.util.ArrayList;
import java.util.List;
public class SplitDataPailStructure extends DataPailStructure {
    
    public static HashMap<Short, FieldStructure> validFieldMap =
            new HashMap<Short, FieldStructure>();
    
    static {
        for(DataUnit._Fields k: DataUnit.metaDataMap.keySet()) {
            FieldValueMetaData md = DataUnit.metaDataMap.get(k).valueMetaData;
            FieldStructure fieldStruct;
            if(md instanceof StructMetaData &&
                    ((StructMetaData) md).structClass.getName().endsWith("Property"))
            {
                fieldStruct = new PropertyStructure(
                ((StructMetaData) md).structClass);
            } else {
                fieldStruct = new EdgeStructure();
            }
            validFieldMap.put(k.getThriftFieldId(), fieldStruct);
        }
    }
    

    @Override
    public List<String> getTarget(Data object) {
        List<String> ret = new ArrayList<String>();
        DataUnit du = ((Data)object).getDataunit();
        short id = du.getSetField().getThriftFieldId();
        ret.add("" + id);
        validFieldMap.get(id).fillTarget(ret, du.getFieldValue());
        return ret;
}

    @Override
    public boolean isValidTarget(String[] dirs) {
        if (dirs.length == 0) return false;
        try {
            short id = Short.parseShort(dirs[0]);
            FieldStructure s = validFieldMap.get(id);
            if (s==null) return false;
            else return s.isValidTarget(dirs);
        } catch (NumberFormatException e) {
            return false;
          }   
}
}
