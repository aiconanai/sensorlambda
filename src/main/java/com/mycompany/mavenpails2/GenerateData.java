/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;


/**
 *
 * @author fedora
 */
public class GenerateData {
   public static Data setValue(long sensorId, int timestamp, double value)
    {
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        SensorPropertyValue sensorpropertyvalue = new SensorPropertyValue(SensorPropertyValue._Fields.VALUE, value);       
        SensorProperty sensorproperty = new SensorProperty(sensor, sensorpropertyvalue);
        DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sensorproperty);

        return new Data(pedigree, dataunit);
    } 
    
      public static Data setTipo(long sensorId, int timestamp, int tipo)
    {
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        SensorPropertyValue sensorpropertyvalue = new SensorPropertyValue();
        if(tipo == 1) sensorpropertyvalue.setTipo(SensorType.Acelerometro);
        else if(tipo == 2) sensorpropertyvalue.setTipo(SensorType.Anemometro);
        else if(tipo == 3) sensorpropertyvalue.setTipo(SensorType.Termometro);
        else sensorpropertyvalue.setTipo(null);
        SensorProperty sensorproperty = new SensorProperty(sensor, sensorpropertyvalue);
        DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sensorproperty);

        return new Data(pedigree, dataunit);
    }
      
         public static Data setPos(long sensorId, int timestamp, int eje1, int eje2,
                                   int elevacion, int posx, int posy, int posz)
    {
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        SensorPropertyValue sensorpropertyvalue = new SensorPropertyValue();
        Position pos = new Position(posx, posy, posz, eje1, eje2, elevacion);
        sensorpropertyvalue.setPos(pos);
        SensorProperty sensorproperty = new SensorProperty(sensor, sensorpropertyvalue);
        DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sensorproperty);

        return new Data(pedigree, dataunit);
    }
    public static Data getColumns (long sensorId, int timestamp, long value, 
                                  int posx, int posy, int posz, int eje1,
                                  int eje2, int elevacion, int tipo)
     {
        
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        
        Position pos = new Position(posx, posy, posz, eje1, eje2, elevacion);
        SensorPropertyValue spv = new SensorPropertyValue();
        
        if (tipo==1) spv.setTipo(SensorType.Acelerometro);
        else if (tipo ==2 ) spv.setTipo(SensorType.Anemometro);
        else if (tipo == 3) spv.setTipo(SensorType.Termometro);
        else spv.setTipo(null);
        spv.setPos(pos);
        spv.setValue(value);
        
        SensorProperty sp = new SensorProperty(sensor, spv);
        DataUnit du = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sp);
        return new Data(pedigree, du);
    
}
}
