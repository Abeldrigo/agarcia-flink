package cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 *   argumentos => src/main/resources/el_quijote.txt
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //Archivo de Salida
    val writer =new PrintWriter(new File(args(1)))
    //Para separar los bloques.
    val separador = "\n**************************************************************************\n"
    // Detecta los archivos en la carpeta de args
    val files = getListOfFiles(args(0))
    /*Invoca la función testaga que obtiene una lista con el nombre de los archivos
      en la ruta especificada y el conteo de los mismos, */
    val result1 = testaga(files)
    println(result1)
    writer.write(result1 + separador)

var idx = 0
    var totalWordsRead = 0
    var file = 0
    var result = new ListBuffer[String]()
    for(file <- files)
    {
      // set up the execution environment
      val env = ExecutionEnvironment.getExecutionEnvironment

      // get input data
      val text = env.readTextFile(file.getPath())

      val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
        .map { (_, 1) }
        .sum(1)

      val total = counts.collect()
      val (llave, valor) = total(0)

      idx += 1
      result += ("#totalWordsOfFile" + idx + " " + file.getName + ": " + valor)

      totalWordsRead += valor

    }
    println(result)
    println("#TotalWordsRead(" + totalWordsRead + ")")
    writer.write(result + separador)
    writer.write("#TotalWordsRead(" + totalWordsRead + ")" + separador)



    //Invoca la función wordCount
    files.foreach(file => wordCount(args(0), file.getName, writer))
    //Cierra el archivo de salida.
    writer.close()
  }

  /*Función que obtiene una lista con el nombre de los archivos en la ruta especificada
  y el conteo de los mismos*/
  private def testaga(files: List[File]) = {
    var result = new ListBuffer[String]()
    files.foreach(file => result += (file.getName))
    result += "#Files(" + files.length.toString + ")"
    //println(result.toList)
  }

  // Función que obtiene una lista con los archivos que se encuentran en una ruta indicada.
  // Para efectos de esta práctica se configuran en Program Arguments del IDE.
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList

    } else {
      List[File]()
    }
  }

  /*Funcion que calcula la frecuencia de las palabras en una colección de texto.
  El algoritmo funciona en dos pasos:
  Primero, el texto es separado en palabras individuales.
  Seguno, se agrupan las palabras y se cuentan.  */
  def wordCount(ruta: String, archivo: String, writer: PrintWriter) {
    // Configura el entorno de ejecución
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Obtiene los datos de entrada
    val text = env.readTextFile(ruta + archivo)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    //Imprime salto de línea
    println()
    //Imprime el nombre del archivo.
    println(archivo)
    //Imprime el resultado del conteo de palabras.
    counts.print()

    writer.write(archivo + "\n")
    writer.write(counts.collect() + "\n\n")

  }
}
