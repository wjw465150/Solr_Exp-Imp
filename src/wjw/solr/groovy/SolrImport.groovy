package wjw.solr.groovy

import java.text.SimpleDateFormat

/**
 * 
 * 用法: /data/app/groovy/bin/groovy /data/app/groovy/script/SolrImport.groovy -u "http://T1:8983/solr" -c core2 -f "/opt/solr_export(core2)-2014-11-04_14-52-16.zip"
 *
 */

//创建 CliBuilder 实例，并定义命令行选项
def cmdline = new CliBuilder(width: 200, usage: 'groovy /data/app/groovy/script/SolrImport.groovy -u "http://T1:8983/solr" -c core2 -f "/opt/solr_export(core2)-2014-11-04_14-52-16.zip"',header:"Options:");
cmdline.h( longOpt: "help", required: false, "show usage information" );
cmdline.u( argName: "solrURL", required: true, args: 1, "solr URL" );
cmdline.c( argName: "coreName", required: true, args: 1, "core name" );
cmdline.f( argName: "exportFile", required: true, args: 1, "export File" );

def opt = cmdline.parse(args);
if (!opt) { return; }
if (opt.h) {
  cmdline.usage();
  return;
}

//将命令行选项赋值给变量
def solrURL=opt.u;
def collectionName=opt.c;
def expFile = opt.f;
expFile = (new File(expFile)).canonicalPath;
def destPath = (new File(expFile)).parentFile.canonicalPath;
destPath=destPath+"/solr_tmp"

//开始获取
try {
  println "solr import start..."
  println "solrUrl: ${solrURL} coreName: ${collectionName} exportFile: ${expFile}";

  def ant = new AntBuilder();
  //解压
  ant.echo("UnZip ${expFile} to ${destPath}");
  ant.unzip ( overwrite: true, src: expFile, dest: destPath )


  (new File(destPath)).eachFile(){f ->
    println "[${getCurrent()}] import ${f}";
    String data = f.getText("UTF-8");
    doPostProcess("${solrURL}/${collectionName}/update", 60*1000, 180*1000, data);
    doPostProcess("${solrURL}/${collectionName}/update", 60*1000, 180*1000, '{"commit": {"softCommit": false}}');
  }

  println "solr import finished!"
} finally {
  ant.delete(dir: destPath)
}

///////////////////////////////////////////////////////////////////////////////////
private String getCurrent() {
  return (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")).format(new java.util.Date())
}


/**
 * 处理HTTP的POST请求,如果不需要BASIC验证,把user以及pass设置为null值
 *
 * @param urlstr
 *          请求的URL
 * @param data
 *          入队列的消息内容
 * @param user
 *          用户名
 * @param pass
 *          口令
 * @return 服务器的返回信息
 * @throws IOException
 */
private String doPostProcess(String urlstr, int connectTimeout, int readTimeout, String data) throws IOException {
  String UTF_8 = "UTF-8"; //HTTP请求字符集

  URL url = new URL(urlstr);

  HttpURLConnection conn = null;
  BufferedReader reader = null;
  OutputStreamWriter writer = null;
  try {
    conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(connectTimeout);
    conn.setReadTimeout(readTimeout);
    conn.setUseCaches(false);
    conn.setDoOutput(true);
    conn.setDoInput(true);
    conn.setRequestProperty("Accept", "*/*");
    conn.setRequestProperty("Content-Type", "application/json;charset=" + UTF_8);
    conn.connect();

    writer = new OutputStreamWriter(conn.getOutputStream(), UTF_8);
    writer.write(data);
    writer.flush();

    if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
      reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), UTF_8));
    } else {
      reader = new BufferedReader(new InputStreamReader(conn.getErrorStream(), UTF_8));
    }
    String line;
    StringBuilder result = new StringBuilder();

    int i = 0;
    while ((line = reader.readLine()) != null) {
      i++;
      if (i != 1) {
        result.append("\n");
      }
      result.append(line);
    }
    return result.toString();
  } finally {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException ex) {
      }
    }

    if (writer != null) {
      try {
        writer.close();
      } catch (IOException ex) {
      }
    }

    if (conn != null) {
      try {
        conn.disconnect();
      } catch (Exception ex) {
      }
    }
  }

}
