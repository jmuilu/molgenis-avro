package org.molgenis.avro

import java.io.File
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.reflect.AvroSchema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import scala.collection.JavaConversions._
import org.apache.avro.compiler.idl.Idl
import org.apache.avro.compiler.specific.SchemaTask
import org.apache.avro.Schema
import org.apache.avro.Protocol
import org.apache.avro.reflect.AvroMeta
import org.molgenis.excel.Workbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.Schema.Field
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.LazyLogging

class Avro2EMX(val typeMap: Map[String, String]) extends LazyLogging {
  val workbook = new Workbook(new XSSFWorkbook());
  val packages = workbook.createSheet("packages")
  val entities = workbook.createSheet("entities")
  val attributes = workbook.createSheet("attributes")
  val tags = workbook.createSheet("tags")

  packages.addRow("name", "description", "parent", "tags");
  entities.addRow("name", "package", "description", "label", "extends", "tags")
  attributes.addRow("name", "entity", "dataType", "refEntity", "nillable", "enumOptions", "description","tags")

  def fixid(proto: Schema) = proto.getNamespace + "." + proto.getName
  def fixid(proto: Protocol) = proto.getNamespace //+"."+proto.getName  

  def  isNullable ( schema : Schema) : String = {
    if( schema.getType == UNION &&  schema.getTypes.exists( x=> x.getType==NULL) ) {
      "TRUE"
    } else {
      "FALSE"
    }
  }
  
  def toEmxType(schema: Schema): Option[String] = {
    val tpe = schema.getType match {
      case UNION =>
        if (schema.getTypes.length > 2 || schema.getTypes.head.getName != "null") {
          logger.error("Cannot handle complex unions. See: " + schema.getName)
          schema.getType
        } else {
          schema.getTypes.tail.head.getType
        }
      case _ => { schema.getType }
    }
    val name = tpe.getName.toLowerCase()

    if (typeMap.isDefinedAt(name)) {
      return Some(typeMap.get(name).get)
    } else {
      return None;
    }
  }

  /**
   * Add Field
   */
  def addField(tpe: Schema, field: Field): Unit = {

    val (emxType,ref, enums) = if ( toEmxType(field.schema()).isDefined) {
      (toEmxType(field.schema()).get,"","")
    } else {
      field.schema().getType match {
    	  case ARRAY => {
          val emx= toEmxType(field.schema.getElementType)
    		  ("mref", "ARRAY"+emx,"")
        }
        case MAP => {
          val emx= toEmxType(field.schema().getValueType)
          ("mref", "MAP"+emx,"")
        }
        case ENUM => {
          ("enum", "",field.schema().getEnumSymbols.mkString(","))
        }
        
        case RECORD=> {
          ("xref",field.schema().getFullName ,"")
        }
        case _ => {
          logger.error("UNKNOWN TYPE: "+field.schema().getType)
          ("","","")
        }
      }
    }

    // attributes.addRow("name", "entity", "dataType", "refEntity", "nillable", "enumOptions", "description","tags")
    attributes.addRow(field.name, tpe.getFullName, emxType, ref,isNullable(field.schema()), enums,tpe.getDoc)
  }

  /**
   * Add Record
   */
  def addRecord(pro: Protocol, tpe: Schema): Unit = {
    entities.addUniqueRow(tpe.getFullName, tpe.getNamespace, "", tpe.getName)
    tpe.getFields.foreach {
      f =>
        addField(tpe, f)
    }
  }

  /**
   * Add package
   */
  def addProtocol(proto: Protocol): Unit = {
    packages.addUniqueRow(fixid(proto)) //add protocols as tags
    proto.getTypes.filter(t => t.getType == RECORD).foreach {
      t =>
        addRecord(proto, t)
    }
  }

}

object Avro2EMX {

  def main(args: Array[String]): Unit = {

    val file = new File("src/test/resources/avro/ga4gh/variants.avdl")
    if (!file.exists()) print("file not found")
    val parser = new Idl(file)
    val protocol = Protocol.parse(parser.CompilationUnit.toString)
    println("Proto " + protocol.getName + " " + protocol.getNamespace)

    val cfgFile = new File("src/main/resources/avro2emx.conf")
    val conf = ConfigFactory.parseFile(cfgFile)

    val cfs = conf.getConfigList("types")
    val mapping = Map(cfs.map {
      c =>
        (c.getString("name").toLowerCase(), c.getString("emx"))
    }.toList: _*)

    val a = new Avro2EMX(mapping)
    a.addProtocol(protocol)
    a.workbook.write("test.xlsx")

    protocol.getTypes.foreach {
      m =>
        println("**" + m.getNamespace + " " + m.getFullName + " " + m.getType + " ")
        if (m.getType == RECORD) {
          m.getFields().foreach {
            f =>
              println(" " + f.name + " type=" + f.schema().getType + " " + "  all=: " + f)
              f.schema().getType match {
                case UNION => {
                  println("  union value = " + f.schema().getTypes.head.getType.getName+ " "+ f.schema().getTypes.tail.head.getType.getName)
                }
                case ARRAY => {
                  println("  arr value = " + f.schema().getElementType.getFullName)
                }
                case ENUM => {
                  println("  enum value = " + f.schema().getEnumSymbols.mkString(","))
                }
                case MAP => {
                  println("   map value = " + f.schema().getValueType.getType)
                }
                case _ => {
                  println("    xxxxxxxx = "+f)
                }
              }

          }
        }
    }

  }
}