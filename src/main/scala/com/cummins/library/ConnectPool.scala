package com.cummins.library

import java.sql.{Connection, DriverManager, SQLException}
import java.util
import java.util.Properties

object ConnectPool {
  //静态的Connection队列
  private var connectionLinkedList: util.LinkedList[Connection] = _

  //此处应当使用keystore方式进行加密
  //val driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  //val dbURL = "jdbc:sqlserver://dl-dev-sqlserver.database.chinacloudapi.cn:1433;database=DataIngestion"
  private val conf = new Properties()
  conf.load(this.getClass.getClassLoader.getResourceAsStream("dbconfig.properties"))

  val getConf: String => String = (str: String) => conf.getProperty(str)
  def getProperties = this.conf

  private val dbURL = getConf("url")

  try
    Class.forName(getConf("driver"))
  catch {
    case e: ClassNotFoundException =>
      e.printStackTrace()
  }

  /**
    * 获取连接,多线程访问并发控制
    *
    * @return conn
    */
  def getConnection: Connection = synchronized {
    if (connectionLinkedList == null) {
      connectionLinkedList = new util.LinkedList[Connection]
      for (i<-0 to 5) {
        try {
          val conn: Connection = DriverManager.getConnection(dbURL, getConf("user"), getConf("password"))
          connectionLinkedList.push(conn)
        } catch {
          case e: SQLException =>
            e.printStackTrace()
        }
      }
    }
    connectionLinkedList.poll
  }

  /**
    * 归还一个连接
    */
  def returnConnection(conn: Connection): Unit = {
    connectionLinkedList.push(conn)
  }

}
