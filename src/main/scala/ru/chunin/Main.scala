package ru.chunin

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    val uri = URI.create("hdfs://localhost:9000")
    val fileSystem = FileSystem.get(uri, conf)
    val recursiveFileSize = getRecursiveFileSize(fileSystem, new Path("/"))
    println(recursiveFileSize)
  }


  @throws[IOException]
  private def getRecursiveFileSize(fileSystem: FileSystem, currentPath: Path): Long = {
    var result: List[Long] = List()
    val locatedFileStatusRemoteIterator = fileSystem.listLocatedStatus(currentPath)
    while (locatedFileStatusRemoteIterator.hasNext) {
      val next = locatedFileStatusRemoteIterator.next
      result = (
        if (next.isDirectory) getRecursiveFileSize(fileSystem, next.getPath)
        else getFileSize(fileSystem, next.getPath)
      ) :: result
    }
    result.sum
  }

  @throws[IOException]
  private def getFileSize(fileSystem: FileSystem, filePath: Path) = {
    val fileStatus = fileSystem.getFileStatus(filePath)
    fileStatus.getLen
  }
}
