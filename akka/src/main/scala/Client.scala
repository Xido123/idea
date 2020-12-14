import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.io.StdIn

class Client(val host:String,port:Int) extends Actor{

  var serverRef:ActorSelection =_

  override def  preStart(): Unit = {

    serverRef = context.actorSelection(s"akka.tcp://Server@${host}:${port}/user/liantongmishu")

  }

  override def receive: Receive = {

    case "start" => println("客户端已经启动")
    case message: String => serverRef ! ClientMessage(message)
    case ServerMessage(message) => println(s"收到服务端的会话： ${message}")
  }
}

object Client {
  def main(args: Array[String]): Unit = {
    val serverHost = "127.0.0.1"
    val serverPort = 8887

    val host = "127.0.0.1"
    val port = 8889
    val config:Config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = $host
         |akka.remote.netty.tcp.port = $port
      """.stripMargin)

    val client: ActorSystem = ActorSystem("client", config)
    val yonghu: ActorRef = client.actorOf(Props[Client](new Client(serverHost, serverPort)), "yonghu")

    yonghu ! "start"

    while (true){
      val str: String = StdIn.readLine()
      yonghu ! str
    }
  }
}

