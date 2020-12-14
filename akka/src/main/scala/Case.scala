//服务端发送给客户端的消息
case class ServerMessage(message:String)
//客户端发送给服务端的消息
case class ClientMessage(message:String)
