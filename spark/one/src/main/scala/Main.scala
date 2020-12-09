import akka.util.LineNumbers.SourceFile
import org.apache.flink.api.scala._

object Main {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val l = List("a b c a","aa ccc dd")
    val inputStream = env.fromCollection(l)

    val result1  = inputStream.map(_.split(" "))
      .flatMap(l=>
        l
      )


    result1.print()
//    env.execute("go")

  }
}
