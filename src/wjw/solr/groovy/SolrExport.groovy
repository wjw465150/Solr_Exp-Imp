package wjw.solr.groovy

import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

import java.text.SimpleDateFormat

/**
 * 
 * 用法: /data/app/groovy/bin/groovy /data/app/groovy/script/SolrExport.groovy -u "http://T1:8983/solr" -c core2 -m 10 -d /opt
 *
 */

//创建 CliBuilder 实例，并定义命令行选项
def cmdline = new CliBuilder(width: 200, usage: 'groovy /data/app/groovy/script/SolrExport.groovy -u "http://T1:8983/solr" -c core2 -m 10 -d /opt',header:"Options:");
cmdline.h( longOpt: "help", required: false, "show usage information" );
cmdline.u( argName: "solrURL", required: true, args: 1, "solr URL" );
cmdline.c( argName: "coreName", required: true, args: 1, "corename" );
cmdline.m( argName: "maxDocPerFile", required: true, args: 1, "max Documents Per File,default: 10000" );
cmdline.d( argName: "destPath", required: false, args: 1, "export file destination Path,default:current path" );

def opt = cmdline.parse(args);
if (!opt) { return; }
if (opt.h) {
  cmdline.usage();
  return;
}

def fieldNames="*";
//this must be URL encoded. *%3A* is the equivalent of a full search using *:*
def query="*%3A*";

//将命令行选项赋值给变量
def solrURL=opt.u;
def collectionName=opt.c;

long maxDocPerFile=10000;
if (opt.m) {
  maxDocPerFile = opt.m.toLong();
}

def destPath = ".";
if (opt.d) {
  destPath = opt.d;
}
destPath = (new File(destPath)).canonicalPath;

//开始获取
println "solr export start..."
println "solrUrl: ${solrURL} coreName: ${collectionName} maxDocPerFile: ${maxDocPerFile}";

def maxDocsUrl = "${solrURL}/${collectionName}/select?wt=json&q=${query}&rows=0".toURL();
def status = maxDocsUrl.getText(["connectTimeout":60*1000,"readTimeout":60*1000]);
long maxDocs = new JsonSlurper().parseText(status)."response"."numFound";
println "maxDocs: ${maxDocs}";


String cursorMark="*";
long fileNumber=0;
String outputFilename="";
String baseDocsUrl = "${solrURL}/${collectionName}/select?wt=json&indent=false&sort=id+asc&q=${query}&fl=${fieldNames}&rows=${maxDocPerFile}";

def docsUrl;
def jsonStatus;
def docs;
while(true) {
  docsUrl = (baseDocsUrl+"&cursorMark=${cursorMark}").toURL();
  status = docsUrl.getText(["connectTimeout":60*1000,"readTimeout":60*1000]);
  jsonStatus = new JsonSlurper().parseText(status);
  cursorMark = jsonStatus."nextCursorMark";
  docs = jsonStatus."response"."docs";
  if(docs.size()==0) {
    println "solr export finished!"
    break;
  }

  def json = new JsonBuilder();
  json(
      docs.collect() { item ->
        item.put("_version_",0);
        item
      }
      )

  fileNumber=fileNumber+1;
  outputFilename="${destPath}/solr_export(${collectionName})-${fileNumber}.json";
  println "[${getCurrent()}] Writing file: ${outputFilename}";
  (new File(outputFilename)).write(JsonOutput.prettyPrint(json.toString()), "UTF-8");
}

//打包压缩
def destFile="${destPath}/solr_export(${collectionName})-"+getCurrent()+".zip";
def ant = new AntBuilder();
ant.echo("Zip solr_export(${collectionName})-*.* files start...");
ant.zip( destfile: destFile, compress: true ) {
  fileset( dir: destPath, includes: "solr_export(${collectionName})-*.*" )
}

ant.delete() {
  fileset( dir: destPath, includes: "solr_export(${collectionName})-*.json" )
}
ant.echo("Zip solr_export(${collectionName})-*.* files finished!");

private String getCurrent() {
  return (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")).format(new java.util.Date())
}
