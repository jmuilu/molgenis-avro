package org.molgenis.excel
import java.io.FileInputStream
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFSheet
import java.io.FileOutputStream
import scala.collection.mutable.HashMap
import com.typesafe.scalalogging.slf4j.LazyLogging

class Workbook(workbook: XSSFWorkbook) extends LazyLogging {
  def createSheet(name: String) = new Sheet(workbook.createSheet(name))

  def write(fileName: String) = {
    val fileOut = new FileOutputStream(fileName);
    workbook.write(fileOut)
    fileOut.close()
  }
}

class Sheet(xsheet: XSSFSheet) extends LazyLogging {
  var currentRow = 0
  var hashmap = new HashMap[String, String]
  val sheet = xsheet
  
  def addUniqueRow(id: String, values: String*): Unit = {
    if (hashmap.isDefinedAt(id)) {
      logger.info("Value " + id + " already stored")
      return
    }
    hashmap.put(id, "K")
    addRow( (id +: values): _*)
  }
  def addRow(values: String*): Unit = {
    /** first value is identifier*/

    val row = sheet.createRow(currentRow);
    var cellId = 0
    values.foreach {
      v =>
        val cell = row.createCell(cellId)
        if (v.trim() != "") cell.setCellValue(v)
        cellId = cellId + 1
    }
    currentRow = currentRow + 1;
  }

}
