package com.example

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MyServer extends Logging{
  def main(args: Array[String]): Unit = {
    if (args.length < 1){
      logError(
        """
          |请输入正确的参数
          |""".stripMargin)
      System.exit(1)
    }
    logInfo("param = "+args(0))

    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    run(sparkSession,args(0))
    sparkSession.stop()
  }

  def run(sparkSession: SparkSession, arg: String):Unit = {
    val param: JSONObject = JSON.parseObject(arg)
    val inputPath = param.getString("input_path")
    val dataType = param.getString("data_type")
    if("json".equals(dataType)){
      val df = sparkSession.read.json(inputPath)
      logWarning("count = "+df.count())
    }else if("parquet".equals(dataType)){
      val df = sparkSession.read.parquet(inputPath)
      logWarning("count = "+df.count())
    }
  }

}
