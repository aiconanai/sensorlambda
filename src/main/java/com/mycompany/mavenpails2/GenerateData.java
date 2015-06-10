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
    public static Data getValue(long sensorId, int timestamp, long value)
    {
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        SensorPropertyValue sensorpropertyvalue = new SensorPropertyValue(SensorPropertyValue._Fields.VALUE, value);       
        SensorProperty sensorproperty = new SensorProperty(sensor, sensorpropertyvalue);
        DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sensorproperty);

        return new Data(pedigree, dataunit);
    }
    
    public static Data getColumns (long sensorId, int timestamp, long value, 
                                  int posx, int posy, int posz, int tipo)
     {
        
        Pedigree pedigree = new Pedigree(timestamp);
        Sensor sensor = new Sensor(Sensor._Fields.SENSOR_ID, sensorId);
        Position pos = new Position(posx, posy, posz);
        if (tipo == 1){
            System.out.println("ENTRE A ESTE CASE");
            SensorPropertyValue spv = new SensorPropertyValue();
            
            spv.setPos(pos);
            
            spv.setTipo(SensorType.Acelerometro);
            spv.setValue(value);
            
            SensorProperty sp = new SensorProperty(sensor, spv);
            DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sp);
            return new Data(pedigree, dataunit);
        }
        else if(tipo == 2){
            SensorPropertyValue spv = new SensorPropertyValue();
            spv.setPos(pos);
            spv.setTipo(SensorType.Dinamometro);
            spv.setValue(value);
            SensorProperty sp = new SensorProperty(sensor, spv);
            DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sp);
            return new Data(pedigree, dataunit);
        }
        else if(tipo == 3){
            SensorPropertyValue spv = new SensorPropertyValue();
            spv.setPos(pos);
            spv.setTipo(SensorType.Termometro);
            spv.setValue(value);
            SensorProperty sp = new SensorProperty(sensor, spv);
            DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sp);
            return new Data(pedigree, dataunit);
        }
        else{
            SensorPropertyValue spv = new SensorPropertyValue();
            spv.setPos(pos);
            spv.setValue(value);
            System.out.println("ERROR: Tipo de sensor incorrecto");
            SensorProperty sp = new SensorProperty(sensor, spv);
            DataUnit dataunit = new DataUnit(DataUnit._Fields.SENSOR_PROPERTY, sp);
            return new Data(pedigree, dataunit);
        }
    }
}
