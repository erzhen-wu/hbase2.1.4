package erzhen.test;

import org.junit.Test;

import java.util.Random;

/***
 * 测试hbaseget方法
 */
public class TestHGet {
    String tableName = "testf";


    /*  String[] family = {"name", "sex", "occupation"};*/
    String[] family = {"cf1"};


    String[] name = {"张三", "李四", "王五"};
    String age;    //年龄
    String[] sex = {"男", "女", "其他"};  //性别
    String[] creeer = {"程序员", "教师", "律师"}; //职业
    Random rand = new Random();
    HbaseUtils hbUtls = HbaseUtils.getInstance();

    @Test
    public void TestPutAdd() throws Exception {
        //  hbUtls.createTable(tableName,family);

        hbUtls.putData(tableName,"0123413","cf1","age","31");
    }

}
