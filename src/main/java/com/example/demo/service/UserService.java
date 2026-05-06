package com.example.demo.service;

import java.sql.*;

public class UserService {

    private String dbUrl = ""jdbc:mysql://localhost:3306/test"";
    private String dbUser = ""root"";
    private String dbPass = ""password"";

    public String getUserByUsername(String username) {
        String result = """";
        try {
            Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            Statement stmt = conn.createStatement();

            String query = ""SELECT * FROM users WHERE username = '"" + username + ""'"";
            ResultSet rs = stmt.executeQuery(query);

            while (rs.next()) {
                result = rs.getString(""username"");
            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
