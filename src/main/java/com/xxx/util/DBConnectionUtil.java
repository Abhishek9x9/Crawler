package com.xxx.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DBConnectionUtil
{
    private static PropReader            proReader;
    
    public static void init(PropReader propReader){
        proReader = propReader; 
    }

    public static Connection getBigBasketDbConnection() throws SQLException
    {
        log.info(">>>>Opening bb-db Connection");
        Connection con = null;
        String url = "jdbc:postgresql://" + proReader.getValue("bb.db.host") + ":" + proReader.getValue("bb.db.port")
                + "/" + proReader.getValue("bb.db.name");
        String user = proReader.getValue("bb.username");
        String password = proReader.getValue("bb.password");
        con = DriverManager.getConnection(url, user, password);
        return con;
    }  
}
