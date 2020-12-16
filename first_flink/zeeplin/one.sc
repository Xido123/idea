import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

val env = ExecutionEnvironment.getExecutionEnvironment

val one = env.fromCollection(List("1,2,3,4","2,2,2,2"))

val out = one.flatMap(_.split(","))
println(out)

