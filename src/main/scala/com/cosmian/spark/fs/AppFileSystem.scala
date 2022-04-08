package com.cosmian.spark.fs

import com.cosmian.spark.AppException
import java.io.OutputStream
import java.io.InputStream

trait AppFileSystem {
  @throws[AppException]
  def writeFile(filePath: String, bytes: Array[Byte])

  @throws[AppException]
  def readFile(filePath: String): Array[Byte]

  @throws[AppException]
  def getOutputStream(filePath: String, overwrite: Boolean): OutputStream

  @throws[AppException]
  def getInputStream(filePath: String): InputStream

  @throws[AppException]
  def listFiles(directoryPath: String): Iterator[String]

  @throws[AppException]
  def isDirectory(directoryPath: String): Boolean
}
