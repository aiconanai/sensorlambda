/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author fedora
 */
public class PartitionedLoginPailStructure extends LoginPailStructure{
    
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    
    @Override
    public List<String> getTarget(Login object) {
      ArrayList<String> directoryPath = new ArrayList<>();
      Date date = new Date(object.loginUnixTime * 1000L); 
      directoryPath.add(formatter.format(date));
      return directoryPath;
    }
    
    @Override
    public boolean isValidTarget(String... strings) {
      if(strings.length != 2) return false;
      try {
          return (formatter.parse(strings[0]) != null);
      } catch(ParseException e){
          return false;
      }
    }
}
