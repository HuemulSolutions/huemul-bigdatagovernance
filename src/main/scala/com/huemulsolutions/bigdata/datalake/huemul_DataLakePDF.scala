package com.huemulsolutions.bigdata.datalake

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

class huemul_DataLakePDF {
  var RDD_Base: Array[String] = null
  var RDDPDF_Data: Array[(Int, Int, Int, String)] = null
  var RDDPDF_Metadata: Array[(String, String)] = null
  
  def openPDF(pdfFile:  Array[(String, org.apache.spark.input.PortableDataStream)], lineDelimiter: String) {
    var i: Int = 0
    
    val _PDF_handler = new BodyContentHandler(-1);
    val _PDF_metadata = new Metadata();
    
    val _PDF_inputstream = pdfFile(0)._2.open()
    val _PDF_pcontext = new ParseContext();
          
    //parsing the document using PDF parser
    val _PDF_pdfparser = new PDFParser();
    val config = _PDF_pdfparser.getPDFParserConfig
    config.setSortByPosition(true)
    _PDF_pdfparser.setPDFParserConfig(config)
    _PDF_pdfparser.parse(_PDF_inputstream, _PDF_handler, _PDF_metadata, _PDF_pcontext)
    
    //get metadata info
    val _PDF_metadataNames = _PDF_metadata.names();
    RDDPDF_Metadata = _PDF_metadataNames.map { x => {(x,_PDF_metadata.get(x))}}
    
    //split lines
    val RDD_Base = _PDF_handler.toString().split(lineDelimiter);
    
    //get RDD extended
    RDDPDF_Data = RDD_Base.map(x =>  {i+=1; (i,x.length,x.trim.length, x)});
  }
    
}