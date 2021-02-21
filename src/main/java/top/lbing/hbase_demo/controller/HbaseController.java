package top.lbing.hbase_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;

import top.lbing.hbase_demo.service.HBaseService;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hbase")
public class HbaseController {

	@Autowired
	HBaseService hBaseService;
	
    @GetMapping("/selectUserByID")
    public String selectUserByID(){
        return hBaseService.getByRowkey("test", "rowkey1");
    }
}
