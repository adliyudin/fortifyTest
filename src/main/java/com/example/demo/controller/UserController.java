package com.example.demo.controller;

import com.example.demo.service.UserService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(""/user"")
public class UserController {

    private UserService userService = new UserService();

    @GetMapping(""/get"")
    public String getUser(@RequestParam String username) {
        return userService.getUserByUsername(username);
    }

    @GetMapping(""/hello"")
    public String hello(@RequestParam String name) {
        return ""<h1>Hello "" + name + ""</h1>"";
    }
}
