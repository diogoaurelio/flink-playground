package com.berlinsmartdata.testutils

import java.io.{BufferedReader, DataInputStream, InputStream, InputStreamReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem


object TestUtils {

  val defaultS3TestBucket = "s3://9-labs"

  /** Hadoop Env - used to talk with AWS S3 */
  val conf = new Configuration()
  //val hadoop_conf_dir = getClass.getResource("/../../../../src/main/resources/hadoop-config/core-site.xml").toString
  val hadoop_conf = this.getClass.getProtectionDomain.getCodeSource().getLocation().getPath() + "/../../../src/main/resources/hadoop-config/core-site.xml"
  conf.addResource(new Path(hadoop_conf))

  val s3fs = new S3AFileSystem()
  val testURI = URI.create(defaultS3TestBucket)
  s3fs.initialize(testURI, conf)

  /** Test utility methods */

  def uuid = java.util.UUID.randomUUID.toString

  def getS3File(filename: String): InputStream = {
    val path = new Path(filename)
    //new DataInputStream(hadoopFileSystem.open(path))
    new DataInputStream(s3fs.open(path))
  }

  def deleteS3File(filename: String): Boolean = {
    val path = new Path(filename)
    if (s3FileExists(filename))
      s3fs.delete(path)
    else
      true
  }

  def s3FileExists(filename: String): Boolean = {
    val path = new Path(filename)
    s3fs.exists(path)
  }

  /**
    * Parses inputStream and asserts it matches expected results
    * @param result
    * @param expectedCollection
    */
  def parseInputStream(result: InputStream, expectedCollection: Seq[String]): Unit = {
    val reader = new BufferedReader(new InputStreamReader(result))

    var line = reader.readLine()
    var counter = 0
    while(line != null) {
      println("Line is: "+line)
      if(line != expectedCollection(counter))
        assert(false)
      counter += 1
      line = reader.readLine()
    }
  }

  def cleanUpFilesTestHelper(path: String) {
    if (new java.io.File(path).exists) {
      try {
        new java.io.File(path).delete()
      } catch {
        case e: Exception => println(s"Failed to cleanup files after test: ${e.getMessage}")
      }
    }
  }

}
