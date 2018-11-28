package com.hashmap

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Utils {

  val putDefaultValue: UserDefinedFunction = udf(f = (input: String) => {
    if (input==null || input== "null" || input=="0") {
      "-999"
    }
    else
    {
      input
    }
  })

  val cleanCode: UserDefinedFunction = udf(f = (input: String) => {
    if (input != null && input.contains("\\(") ) {
      input.split("\\(")(0)
    }
    else
    {
      input
    }
  })

  val transformUppercase: UserDefinedFunction = udf(f = (input: String) => {
    if (input != null) {
      input.toUpperCase
    }
    else
    {
      input
    }
  })

  val extractYear: UserDefinedFunction = udf(f = (input: String) => {
    if (input != null && input.contains("/")) {
      val year=input.split("/")(2)
      year
    }
    else
    {
      input
    }
  })

}
