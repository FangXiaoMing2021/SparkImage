package com.fang.spark.task;

/**
 * Created by fang on 16-12-29.
 */
public class NoSqlParserException extends Exception{
    private static final long serialVersionUID = 1L;
    NoSqlParserException()
    {

    }
    NoSqlParserException(String sql)
    {
        //调用父类方法
        super(sql);
    }
}
