/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenpails2;

import java.util.List;

/**
 *
 * @author fedora
 */
public interface FieldStructure {
    public boolean isValidTarget(String[] dirs);    
    public void fillTarget(List<String> ret, Object val);
}
