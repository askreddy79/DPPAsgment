
"# DPPAsgment" 
Get project using git ssh or Download zip. 

1) Extract project files in a folder (C:\askr\DPPAsgment-master)

2) Run below Maven command 

     C:\askr\DPPAsgment-master>mvn clean install -DskipTests=true

3) Once the build is successful you will get a 'dpp-1.0-SNAPSHOT-1.0-SNAPSHOT.tar.gz' file in 'C:\askr\DPPAsgment-master\target' folder.

4) Move this 'tar.gz' file on hadoop cluster and extract it using command

   tar -zxvf dpp-1.0-SNAPSHOT-1.0-SNAPSHOT.tar.gz

5) It shows below folder structure after extract above 'tar.gz'

   [hadoop@ip-10-0-0-185 dpp-1.0-SNAPSHOT]$ ll

total 8

drwxrwxr-x 2 hadoop hadoop 4096 Oct 19 22:32 lib

drwxr-xr-x 2 hadoop hadoop 4096 Oct 19 22:35 scripts

6) use the below to submit your spark job.

[hadoop@ip-10-0-0-185 scripts]$ ./dppsparkrun.sh <hdfs-folder-zip-files-location>

Eg: ./dppsparkrun.sh /user/hadoop/enron

7) Printed results in logs. Use yarn logs to see results.

