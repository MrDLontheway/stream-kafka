package com.wxstc.dl.util

import java.util

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper

object JsonUtils {
  // 定义jackson对象
  private val MAPPER = new ObjectMapper

  /**
    * 将对象转换成json字符串。
    * <p>Title: pojoToJson</p>
    * <p>Description: </p>
    *
    * @param data
    * @return
    */
  def objectToJson(data: Any): String = {
    try {
      val string = MAPPER.writeValueAsString(data)
      return string
    } catch {
      case e: JsonProcessingException =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 将json结果集转化为对象
    *
    * @param jsonData json数据
    * @param clazz    对象中的object类型
    * @return
    */
  def jsonToPojo[T](jsonData: String, beanType: Class[T]): Option[T] = {
//    MAPPER.readValue(jsonData, beanType)
    try {
      val t = MAPPER.readValue(jsonData, beanType)
      Some(t)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  /**
    * 将json数据转换成pojo对象list
    * <p>Title: jsonToList</p>
    * <p>Description: </p>
    *
    * @param jsonData
    * @param beanType
    * @return
    */
  def jsonToList[T](jsonData: String, beanType: Class[T]): util.List[T] = {
    val javaType = MAPPER.getTypeFactory.constructParametricType(classOf[util.List[_]], beanType)
    try {
      val list = MAPPER.readValue(jsonData, javaType)
      return list
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

}
