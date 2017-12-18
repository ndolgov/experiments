package net.ndolgov.sparkdatasourcetest.connector

import java.io.{File, FileFilter}

/** For simplicity, avoid using apache commons-style dependencies */
object FileUtils {
  def mkDir(path: String): File = {
    val dir = new File(path)
    if (dir.mkdir()) dir else throw new RuntimeException("Could not create dir: " + path)
  }

  def deleteRecursively(file: File): Boolean = {
    if (file.isDirectory) {
      file.listFiles().forall(file => deleteRecursively(file))
    } else {
      if (file.exists) file.delete() else true
    }
  }

  def listSubDirs(path: String): Array[String] = {
    val dir = new File(path)

    if (dir.isDirectory) {
        dir.listFiles(new FileFilter {
          override def accept(subDir: File): Boolean = subDir.getName.startsWith("part-")
        }).map((subDir: File) => subDir.getAbsolutePath)
    } else {
      throw new RuntimeException("Not a dir: " + path)
    }
  }
}
