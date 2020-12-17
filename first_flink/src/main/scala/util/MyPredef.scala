package util

class MyPredef {

  implicit def deletePath(path:String) = new SparkHelloWorld(path)
}
