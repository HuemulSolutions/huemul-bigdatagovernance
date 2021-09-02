package com.huemulsolutions.bigdata.datalake

import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.sax.BodyContentHandler

class HuemulDataLakePDF {
  var rddBase: Array[String] = _
  var rddPdfData: Array[(Int, Int, Int, String)] = _
  var rddPdfMetadata: Array[(String, String)] = _

  def openPdf(pdfFile:  Array[(String, org.apache.spark.input.PortableDataStream)], lineDelimiter: String) {
    var i: Int = 0

    val _PDF_handler = new BodyContentHandler(-1)
    val _PDF_metadata = new Metadata()

    val _PDF_inputstream = pdfFile(0)._2.open()
    val _PDF_pcontext = new ParseContext()

    //parsing the document using PDF parser
    val _pdfParser = new PDFParser()
    val config = _pdfParser.getPDFParserConfig
    config.setSortByPosition(true)
    _pdfParser.setPDFParserConfig(config)
    _pdfParser.parse(_PDF_inputstream, _PDF_handler, _PDF_metadata, _PDF_pcontext)

    //get metadata info
    val _PDF_metadataNames = _PDF_metadata.names()
    rddPdfMetadata = _PDF_metadataNames.map { x => {(x,_PDF_metadata.get(x))}}

    //split lines
    val RDD_Base = _PDF_handler.toString.split(lineDelimiter)

    //get RDD extended
    rddPdfData = RDD_Base.map(x =>  {i+=1; (i,x.length,x.trim.length, x)})
  }
    
}