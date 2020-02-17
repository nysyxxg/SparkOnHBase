package com.cloudera.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.security.UserGroupInformation

class UserSecurityFunction(configuration: Configuration) {

  def login():UserGroupInformation = {
    var UGI:UserGroupInformation = new UserGroupInformation()

    UGI
  }

  def isSecurityEnable(): Boolean ={
    var bl:Boolean = User.isHBaseSecurityEnabled(configuration)
    bl
  }

}
