Solr_Exp-Imp
============

Solr Data Export and import Tools

Export:
  `groovy /data/app/groovy/script/SolrExport.groovy -u "http://T1:8983/solr" -c core2 -m 10 -d /opt`

Import `groovy /data/app/groovy/script/SolrImport.groovy -u "http://T1:8983/solr" -c core2 -f "/opt/solr_export(core2)-2014-11-04_14-52-16.zip"`
