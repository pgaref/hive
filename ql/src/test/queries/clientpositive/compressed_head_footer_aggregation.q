set hive.mapred.mode=nonstrict;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/testcase1;
dfs -copyFromLocal ../../data/files/testcase2.csv.bz2  ${system:test.tmp.dir}/testcase1/;


CREATE EXTERNAL TABLE `testcase1`(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  LOCATION '${system:test.tmp.dir}/testcase1'
  TBLPROPERTIES ("skip.header.line.count"="1", "skip.footer.line.count"="1");


select * from testcase1;

select count(*) from testcase1;
