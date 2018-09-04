import java.util

import com.wxstc.dl.bean.{DyRoomGift, GiftSum, ScalaYuanZu}
import com.wxstc.dl.redis.JedisSingle
import com.wxstc.dl.util.JsonUtils
import com.alibaba.fastjson.JSON

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object Tst {
  def main(args: Array[String]): Unit = {
    var m:util.HashMap[Int, String] = new util.HashMap[Int,String]()
    m.put(1005,"超级火箭")
    m.put(196,"火箭")
    val jedis = JedisSingle.jedisPool.getResource
    val json = jedis.get("dyGiftInFo")

    val res2 = JSON.parseObject(json,classOf[util.HashMap[String, String]])
    val res = JSON.toJSON(m)
    println(res)
    println(res2)

    var b:util.ArrayList[(String, Int)] = new util.ArrayList[(String, Int)]()
    b.add(("chaojihuojian",100))
    b.add(("huojian",50))
    val a:Array[(String, util.ArrayList[(String, Int)])] = Array(("123",b),("345",b))
    val js = JSON.toJSON(a)

    println(js)

//    val s2 = "[{\"_1\":\"123\",\"_2\":[{\"_1\":\"chaojihuojian\",\"_2\":100},{\"_1\":\"huojian\",\"_2\":50}]},{\"_1\":\"345\",\"_2\":[{\"_1\":\"chaojihuojian\",\"_2\":100},{\"_1\":\"huojian\",\"_2\":50}]}]";
//    val ress2 = JSON.parseArray(s2,classOf[util.HashMap[String, String]])
//    println(ress2)

    val drg = new DyRoomGift()
    val gs = new util.ArrayList[GiftSum]()
    gs.add(new GiftSum("huojian",100))
    gs.add(new GiftSum("chaoji",50))
    drg.setGifts(gs)

    println(JSON.toJSON(drg))
  }
}
