import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

class Server extends Actor{
  override def receive: Receive = {
    case "start" => println("服务器已启动")
    case ClientMessage(message) => {
      message match {
        case "话费剩多少钱" => {
          sender() ! ServerMessage("当前余额不足，请充值")
          println("话费剩多少钱")
        }
        case "转换人工服务" => {
          sender() ! ServerMessage("请按0")
          println("转换人工服务")
        }
        case a: Any => {
          sender() ! ServerMessage("服务端已停止响应")
          println(a)
        }
      }
    }
  }
}

object Server{
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1"
    val port = 8887
    val config: Config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
         |""".stripMargin)
    val server: ActorSystem = ActorSystem("Server",config)
    val liantongmishu: ActorRef = server.actorOf(Props[Server], "liantongmishu")
    liantongmishu ! "start"
  }
}
