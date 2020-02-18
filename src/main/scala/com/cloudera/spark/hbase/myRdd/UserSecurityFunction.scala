package com.cloudera.spark.hbase.myRdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.security.UserGroupInformation

class UserSecurityFunction(configuration: Configuration) {

  def login():UserGroupInformation = {
    var UGI:UserGroupInformation = null
    UGI
  }

  def isSecurityEnable(): Boolean ={
    var bl:Boolean = User.isHBaseSecurityEnabled(configuration)
    bl
  }

}
