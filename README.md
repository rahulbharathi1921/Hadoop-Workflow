# Hadoop-Workflow - Windows Install + MapReduce Labs (3.3.6 + Java 8)

One ZIP for a working Windows-native Hadoop setup plus WordCount +
Matrix-Multiplication jobs and Big Data lab guides (Exps 2-8, Hive).

Repo: https://github.com/rahulbharathi1921/Hadoop-Workflow

## What's inside

- README.md (this file)
- Installation/FIle.txt (original rough notes, kept as reference)
- Installation/configs/ (core-site, hdfs-site, mapred-site, yarn-site)
- Installation/bin/ (winutils.exe, hadoop.dll + .cmd shims - copy to C:\Hadoop\bin)
- Installation/DOWNLOADS.md (links for the 700MB+ installers NOT in git)
- 2-4/ (wc_input, matrix_input, wordcount + matrixmult java + jars)
- BigData_Lab_Experiments_2to8.md
- Experiment_5_Hive_Installation_Practice.md

Large installers (hadoop tar.gz ~730MB, jdk ~221MB, VC_redist, 7z)
are NOT committed - GitHub blocks files over 100MB.

## Prerequisites

- Windows 10/11 64-bit, C: with 5GB free, admin rights
- Java 8 (8u202), Hadoop 3.3.6, VC++ Redist (see DOWNLOADS.md)

Check:
  java -version
  hadoop version
## Installation - step by step

### 1. Extract + place folders
Extract Hadoop to C:\Hadoop, install Java to C:\Java. Keep simple
short paths - spaces break Hadoop on Windows.

### 2. Env vars (exact spelling matters)
  HADOOP_HOME=C:\Hadoop
  JAVA_HOME=C:\Java
Add to Path: C:\Hadoop\bin, C:\Hadoop\sbin, C:\Java\bin
Close/reopen CMD after editing.

### 3. VC++ runtime
Install VC_redist.x64.exe first or winutils will fail silently.

### 4. Fix Windows binaries
Copy Installation/bin/ files into C:\Hadoop\bin (winutils.exe,
hadoop.dll + cmds). Double-click winutils.exe - flash open/close
means OK. If MSVCR120.dll error, copy Installation/msvcr120.dll
to C:\Windows\System32 or install VC_redist.

### 5. Point Hadoop at Java 8
Edit C:\Hadoop\etc\hadoop\hadoop-env.cmd line ~25:
  set JAVA_HOME=C:\Java

### 6. Data dirs
  mkdir C:\Hadoop\data\namenode
  mkdir C:\Hadoop\data\datanode

### 7. Edit 4 XMLs (copies in Installation/configs/)
core-site.xml: fs.defaultFS = hdfs://localhost:9000
hdfs-site.xml: dfs.replication=1, namenode dir file:///C:/Hadoop/data/namenode,
  datanode dir file:///C:/Hadoop/data/datanode
mapred-site.xml: mapreduce.framework.name=yarn
yarn-site.xml: yarn.nodemanager.aux-services=mapreduce_shuffle,
  shuffle class org.apache.hadoop.mapred.ShuffleHandler,
  plus yarn.resourcemanager.hostname=localhost

### 8. Format once + start (Admin CMD)
  hdfs namenode -format
  cd C:\Hadoop\sbin
  start-dfs.cmd
  start-yarn.cmd
  jps
Want: NameNode, DataNode, ResourceManager, NodeManager.
UIs: http://localhost:9870 and http://localhost:8088
Never re-format a working namenode - ClusterID mismatch!
## Quick runs (Exps 2-4)

WordCount:
  hdfs dfs -mkdir -p /lab/input
  hdfs dfs -put 2-4\wc_input.txt /lab/input/
  hadoop jar 2-4\wordcount\wordcount.jar WCDriver /lab/input/wc_input.txt /lab/wc_out
  hdfs dfs -cat /lab/wc_out/part-r-00000

Matrix (2x2 * 2x3):
  hdfs dfs -put 2-4\matrix_input.txt /lab/input/
  hadoop jar 2-4\matrixmult\matrixmultiplication.jar MatrixMultiplicationDriver /lab/input/matrix_input.txt /lab/mat_out
  hdfs dfs -cat /lab/mat_out/part-r-00000

Hive/HBase/Thrift/Sqoop (5-8) need WSL2 Ubuntu - see the two .md guides.

## Troubleshooting - common Windows failures

- namenode -format: Invalid directory .../current -> icacls grant:
  icacls C:\Hadoop\data\namenode /grant %USERNAME%:F /T /C (same for datanode, logs)
- jps missing NameNode -> format never succeeded, check :9870 only after success msg
- jps missing ResourceManager, NM retrying :8031 -> set HADOOP_YARN_HOME=C:\Hadoop,
  fix yarn-env.cmd %yarn% typo to %USERNAME%, run yarn resourcemanager foreground
- RM Access denied createFileWithMode0 -> same icacls fix, set hadoop.tmp.dir owned
- winutils missing/MSVCR120 -> use repo bin/, install VC_redist
- JAVA_HOME wrong / Java 17 on PATH -> JAVA_HOME=C:\Java, java -version = 1.8
- 'C:\Hadoop\sbin' not recognized -> cd there first, start-all.cmd is deprecated
- ClusterID mismatch after re-format -> delete both current/ folders, format once
- Ports in use -> stop-all.cmd, taskkill java.exe, netstat check 9000/9870/8088

## Download as ZIP

Code -> Download ZIP on GitHub, extract, follow Installation above.

## Credits

Your Installation/FIle.txt steps + real logs from this machine folded in.
