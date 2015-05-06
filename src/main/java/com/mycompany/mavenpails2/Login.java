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
public class Login {
    public String userName;
    public long loginUnixTime;
    
    public Login(String _user, long _login){
        userName = _user;
        loginUnixTime = _login;
    }  
}
