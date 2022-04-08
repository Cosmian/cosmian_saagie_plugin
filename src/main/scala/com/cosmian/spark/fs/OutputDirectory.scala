package com.cosmian.spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.cosmian.spark.fs.AppFileSystem
import com.cosmian.spark.fs.LocalFileSystem

class OutputDirectory(val fs: AppFileSystem, val directory: Path) {

//   def getFs = {
//     fs
//   }

//   def getDirectory = {
//     directory
//   }

}

object OutputDirectory {

  @throws[AppException]
  def apply(uriString: String) = {

    val uri: URI =
      try {
        new URI(uriString)
      } catch {
        case e: URISyntaxException =>
          throw AppException(
            s"invalid URI for the output directory: $uriString: ${e.getMessage()}",
            e
          );
      }
    val directory = Paths.get(uri.getPath());

    val fs: AppFileSystem =
      if (uri.getScheme() == null || uri.getScheme().equals("file")) {
        new LocalFileSystem()
      } else {
        throw AppException(
          s"unknown scheme for the output directory: $uriString"
        )
      }

    if (!fs.isDirectory(directory.toString())) {
      throw AppException(s"not a directory: $uriString")
    }

    new OutputDirectory(fs, directory)
  }

}
