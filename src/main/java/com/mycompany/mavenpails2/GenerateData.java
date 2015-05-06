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
}
