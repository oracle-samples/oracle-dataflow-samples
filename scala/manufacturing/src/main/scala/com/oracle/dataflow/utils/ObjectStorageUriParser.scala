package com.oracle.dataflow.utils

import com.oracle.dataflow.utils.Constants.URI_PATTERN
import java.util.regex.Matcher

object ObjectStorageUriParser {
  def parse(uri:String) = {
    val matcher:Matcher = URI_PATTERN.matcher(uri)
    if (!matcher.find || matcher.groupCount()!=3) throw new IllegalArgumentException("unknown pattern: " + uri)
    objectStorageDetails.bucket=matcher.group(1).trim()
    objectStorageDetails.namespace=matcher.group(2).trim()
    objectStorageDetails.obj= matcher.group(3).trim()
  }
  object objectStorageDetails {
    var namespace:String=""
    var bucket:String=""
    var obj:String=""
  }
}
