# CCS334 Big Data Analytics Lab — Experiments 2 to 8

You've completed **Experiment 1** (Hadoop installed natively on Windows at `C:\Hadoop-3.3.0`). This guide covers 2–8.

**Setup note:** Experiments 2–4 continue on your existing Windows Hadoop install. For 5–7 (Hive, HBase, Thrift), native Windows installs are unreliable and poorly documented — the standard, fast path is **WSL2 (Windows Subsystem for Linux)**, a lightweight Linux environment inside Windows (not a full VM, much faster to set up). Instructions for enabling it are in Experiment 5 below.

---

## Experiment 2: Hadoop File Management Tasks (Adding, Retrieving, Deleting Files)

Continue using your Windows Hadoop. Make sure your cluster is running first:
```cmd
cd C:\Hadoop-3.3.0\sbin
start-all.cmd
```
Wait ~20 seconds, then open a new Command Prompt for the commands below.

### 1. Create a directory in HDFS
```cmd
hdfs dfs -mkdir /bigdata_lab
hdfs dfs -mkdir /bigdata_lab/input
```

### 2. Add (upload) files from local disk to HDFS
Create a sample file first:
```cmd
echo This is a sample file for HDFS testing > C:\sample.txt
```
Upload it:
```cmd
hdfs dfs -put C:\sample.txt /bigdata_lab/input
```

### 3. List files/directories
```cmd
hdfs dfs -ls /bigdata_lab/input
```

### 4. Retrieve (download) a file from HDFS to local disk
```cmd
hdfs dfs -get /bigdata_lab/input/sample.txt C:\downloaded_sample.txt
```
Check `C:\downloaded_sample.txt` exists locally with the same content.

### 5. View file content directly from HDFS
```cmd
hdfs dfs -cat /bigdata_lab/input/sample.txt
```

### 6. Copy a file within HDFS
```cmd
hdfs dfs -cp /bigdata_lab/input/sample.txt /bigdata_lab/input/sample_copy.txt
```

### 7. Rename/move a file within HDFS
```cmd
hdfs dfs -mv /bigdata_lab/input/sample_copy.txt /bigdata_lab/input/sample_renamed.txt
```

### 8. Delete a file
```cmd
hdfs dfs -rm /bigdata_lab/input/sample_renamed.txt
```

### 9. Delete a directory (recursively)
```cmd
hdfs dfs -rm -r /bigdata_lab/input
```

### 10. Check disk usage / file size
```cmd
hdfs dfs -du -h /bigdata_lab
```

**Result to write in your record:** "HDFS file management operations — add, retrieve, copy, rename, and delete — were performed successfully using HDFS shell commands."

**Screenshot for submission:** terminal showing the `-put`, `-ls`, `-get`, `-cat`, `-rm` command outputs in sequence.

---

## Experiment 3: Matrix Multiplication with Hadoop MapReduce

### Step 1 — Set up a Java project folder
```cmd
mkdir C:\bigdata_lab\matrixmult
cd C:\bigdata_lab\matrixmult
```

### Step 2 — Create input files
Two matrices, stored as `row,col,value` triples (sparse format — standard for MapReduce matrix multiplication). Say Matrix A is 2x2 and Matrix B is 2x3.

Create `C:\bigdata_lab\matrix_input.txt`:
```
A,0,0,1
A,0,1,2
A,1,0,3
A,1,1,4
B,0,0,5
B,0,1,6
B,0,2,7
B,1,0,8
B,1,1,9
B,1,2,10
```

### Step 3 — Write the Mapper
`MatrixMultiplicationMapper.java`:
```java
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends Mapper<Object, Text, Text, Text> {
    // Set these to match your matrix dimensions
    int m = 2; // rows in A
    int n = 2; // cols in A / rows in B
    int p = 3; // cols in B

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        String matrixName = parts[0];
        int row = Integer.parseInt(parts[1]);
        int col = Integer.parseInt(parts[2]);
        double val = Double.parseDouble(parts[3]);

        if (matrixName.equals("A")) {
            for (int k = 0; k < p; k++) {
                context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
            }
        } else { // matrixName.equals("B")
            for (int i = 0; i < m; i++) {
                context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
            }
        }
    }
}
```

### Step 4 — Write the Reducer
`MatrixMultiplicationReducer.java`:
```java
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Double> mapA = new HashMap<>();
        Map<Integer, Double> mapB = new HashMap<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts[0].equals("A")) {
                mapA.put(Integer.parseInt(parts[1]), Double.parseDouble(parts[2]));
            } else {
                mapB.put(Integer.parseInt(parts[1]), Double.parseDouble(parts[2]));
            }
        }

        double result = 0;
        for (Integer k : mapA.keySet()) {
            if (mapB.containsKey(k)) {
                result += mapA.get(k) * mapB.get(k);
            }
        }
        context.write(key, new Text(String.valueOf(result)));
    }
}
```

### Step 5 — Write the Driver
`MatrixMultiplicationDriver.java`:
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplicationDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplicationDriver.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Step 6 — Compile and package into a JAR
```cmd
cd C:\bigdata_lab\matrixmult
javac -classpath "%HADOOP_HOME%\share\hadoop\common\*;%HADOOP_HOME%\share\hadoop\mapreduce\*;%HADOOP_HOME%\share\hadoop\common\lib\*" -d . MatrixMultiplicationMapper.java MatrixMultiplicationReducer.java MatrixMultiplicationDriver.java
jar -cvf matrixmultiplication.jar *.class
```

### Step 7 — Upload input and run
```cmd
hdfs dfs -mkdir /matrix_input
hdfs dfs -put C:\bigdata_lab\matrix_input.txt /matrix_input
hadoop jar matrixmultiplication.jar MatrixMultiplicationDriver /matrix_input /matrix_output
```

### Step 8 — View result
```cmd
hdfs dfs -cat /matrix_output/part-r-00000
```
You'll see output like `0,0  <value>`, `0,1  <value>` etc. — each cell of the resulting matrix.

**Result:** "Matrix multiplication using Hadoop MapReduce was implemented and executed successfully."

---

## Experiment 4: Basic WordCount MapReduce Program

### Step 1 — Set up folder
```cmd
mkdir C:\bigdata_lab\wordcount
cd C:\bigdata_lab\wordcount
```

### Step 2 — Create input text file
```cmd
echo hello world hello hadoop big data analytics hello cloud > C:\bigdata_lab\wc_input.txt
```

### Step 3 — Write the Mapper
`WCMapper.java`:
```java
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
```

### Step 4 — Write the Reducer
`WCReducer.java`:
```java
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
```

### Step 5 — Write the Driver
`WCDriver.java`:
```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "My Word Count Program");
        job.setJarByClass(WCDriver.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### Step 6 — Compile and package
```cmd
cd C:\bigdata_lab\wordcount
javac -classpath "%HADOOP_HOME%\share\hadoop\common\*;%HADOOP_HOME%\share\hadoop\mapreduce\*;%HADOOP_HOME%\share\hadoop\common\lib\*" -d . WCMapper.java WCReducer.java WCDriver.java
jar -cvf wordcount.jar *.class
```

### Step 7 — Run it
```cmd
hdfs dfs -mkdir /sample
hdfs dfs -mkdir /sample/input
hdfs dfs -put C:\bigdata_lab\wc_input.txt /sample/input
hadoop jar wordcount.jar WCDriver /sample/input /sample/output
```

### Step 8 — View results
```cmd
hdfs dfs -cat /sample/output/part-r-00000
```
Expected: each word with its count (e.g. `hello  3`).

**Result:** "The MapReduce program that implements Word Count was executed and verified successfully."

---

# Experiment 5: Installation of Hive + Practice Examples

**Platform:** Windows with WSL2 + Ubuntu\
**Versions used:** Java 8, Hadoop 3.3.6, Hive 3.1.3\
**Purpose:** Install Hive in Ubuntu (WSL2), create a database and table,
load a CSV file, and run basic HiveQL queries.

> **Important:** Run Linux commands inside the Ubuntu terminal, not
> PowerShell. This setup is separate from any Hadoop installation you
> have on Windows. Do not format the NameNode again after HDFS has
> already been initialized.

------------------------------------------------------------------------

## Step 1 --- Enable WSL2 and install Ubuntu (one-time setup)

Open **PowerShell as Administrator**:

``` powershell
wsl --install -d Ubuntu
```

Restart Windows if prompted. Launch Ubuntu and create a Linux username
and password.

Check that Ubuntu uses WSL2 in PowerShell:

``` powershell
wsl --list --verbose
```

The Ubuntu entry should show version `2`.

If WSL is already installed, do not reinstall it; continue in the
existing Ubuntu terminal.

------------------------------------------------------------------------

## Step 2 --- Install Java 8 and Hadoop

### 2.1 Update Ubuntu and install required tools

Run inside Ubuntu:

``` bash
sudo apt update
sudo apt install -y wget curl gpg openssh-server rsync
```

### 2.2 Install Java 8

Some recent Ubuntu releases do not provide `openjdk-8-jdk` in their
default repositories. Install Java 8 from Eclipse Temurin:

``` bash
sudo mkdir -p /etc/apt/keyrings
wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public \
  | gpg --dearmor \
  | sudo tee /etc/apt/keyrings/adoptium.gpg > /dev/null

echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(. /etc/os-release && echo "$VERSION_CODENAME") main" \
  | sudo tee /etc/apt/sources.list.d/adoptium.list

sudo apt update
sudo apt install -y temurin-8-jdk
```

Verify Java:

``` bash
java -version
```

The output should identify Java version `1.8`. If multiple Java versions
are installed, the configuration below explicitly tells Hadoop to use
Java 8.

### 2.3 Download and install Hadoop 3.3.6

``` bash
cd ~
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop
sudo chown -R "$USER":"$USER" /usr/local/hadoop
```

### 2.4 Configure environment variables

Open the shell configuration:

``` bash
nano ~/.bashrc
```

Add these lines at the end:

``` bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HIVE_HOME=/usr/local/hive
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin
```

Save with **Ctrl+O**, press **Enter**, then exit with **Ctrl+X**. Load
the settings:

``` bash
source ~/.bashrc
echo "$JAVA_HOME"
java -version
```

If the Java 8 installation directory differs on your system, find it
with:

``` bash
ls -d /usr/lib/jvm/*8*
```

Use the actual Java 8 directory in `JAVA_HOME`.

Configure Hadoop to use Java 8:

``` bash
nano "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
```

Find the `JAVA_HOME` setting and set it to:

``` bash
export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
```

Use the actual Java 8 directory if it differs.

### 2.5 Configure Hadoop

Set the HDFS default filesystem:

``` bash
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF
```

Set the single-node HDFS directories and replication:

``` bash
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/USER_PLACEHOLDER/hadoopdata/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/USER_PLACEHOLDER/hadoopdata/datanode</value>
  </property>
</configuration>
EOF
```

Replace `USER_PLACEHOLDER` in the file with your Ubuntu username. You
can print it with:

``` bash
whoami
```

For example, if `whoami` prints `rahul`, both paths should begin with
`/home/rahul/`.

Configure MapReduce to use YARN:

``` bash
cat > "$HADOOP_HOME/etc/hadoop/mapred-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF
```

Configure YARN for a single-node lab:

``` bash
cat > "$HADOOP_HOME/etc/hadoop/yarn-site.xml" <<'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
</configuration>
EOF
```

### 2.6 Configure passwordless SSH and start Hadoop

``` bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sudo service ssh start
ssh -o StrictHostKeyChecking=accept-new localhost exit
```

If SSH asks you to confirm the host key, type `yes`.

Create the local data directories:

``` bash
mkdir -p ~/hadoopdata/namenode ~/hadoopdata/datanode
```

**Format the NameNode only on a brand-new installation, once:**

``` bash
hdfs namenode -format
```

Start HDFS and YARN:

``` bash
start-dfs.sh
start-yarn.sh
jps
```

The process list should include `NameNode`, `DataNode`,
`SecondaryNameNode`, `ResourceManager`, and `NodeManager` (as well as
`Jps`).

Create the HDFS directories Hive will use:

``` bash
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod 1777 /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod 777 /user/hive/warehouse
hdfs dfs -mkdir -p "/user/$USER"
hdfs dfs -chmod 755 "/user/$USER"
```

------------------------------------------------------------------------

## Step 3 --- Download and install Hive 3.1.3

``` bash
cd ~
wget https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
tar -xzf apache-hive-3.1.3-bin.tar.gz
sudo mv apache-hive-3.1.3-bin /usr/local/hive
sudo chown -R "$USER":"$USER" /usr/local/hive
```

Reload the environment:

``` bash
source ~/.bashrc
hive --version
```

------------------------------------------------------------------------

## Step 4 --- Initialize the Hive metastore (Derby)

Hive's embedded Derby metastore stores table/database metadata locally.
Initialize it **only once**:

``` bash
schematool -dbType derby -initSchema
```

If the command reports that the schema already exists, do not delete the
metastore or initialize it again; continue to Step 5.

------------------------------------------------------------------------

## Step 5 --- Launch Hive

Ensure Hadoop services are running:

``` bash
jps
```

If they are not running, start them:

``` bash
sudo service ssh start
start-dfs.sh
start-yarn.sh
```

Launch Hive:

``` bash
hive
```

You should see the `hive>` prompt. Hive may display SLF4J binding or
Hive-on-MR deprecation warnings; these are not necessarily fatal if the
prompt appears and queries work.

------------------------------------------------------------------------

## Step 6 --- Create a database and employees table

Run these SQL commands at the `hive>` prompt:

``` sql
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;

CREATE TABLE IF NOT EXISTS employees (
    id INT,
    name STRING,
    salary FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SHOW DATABASES;
SHOW TABLES;
DESCRIBE employees;
```

Keep the Hive terminal open.

------------------------------------------------------------------------

## Step 7 --- Create a CSV file and load it

Open a **second Ubuntu terminal**. Do not type these Linux commands at
the `hive>` prompt.

Create a sample CSV:

``` bash
cat > ~/employees.csv <<'EOF'
1,John,50000
2,Alice,60000
3,Bob,55000
EOF
```

Verify the file:

``` bash
cat ~/employees.csv
```

Return to the first terminal where the `hive>` prompt is open. Confirm
the database is selected, then load the CSV:

``` sql
USE mydatabase;
LOAD DATA LOCAL INPATH '/home/YOUR_UBUNTU_USERNAME/employees.csv' INTO TABLE employees;
```

Replace `YOUR_UBUNTU_USERNAME` with the output of `whoami` from Ubuntu.
For example, if it prints `rahul`, use `/home/rahul/employees.csv`.

Run the queries:

``` sql
SELECT * FROM employees;

SELECT name, salary
FROM employees
WHERE salary > 52000;
```

Expected filtered results:

``` text
Alice   60000
Bob     55000
```

**Note:** If you load the same file more than once, duplicate rows may
appear. To start the practice again, use a fresh table or clear the
table before loading again.

------------------------------------------------------------------------

## Step 8 --- Exit Hive and stop services

Exit the Hive prompt:

``` sql
EXIT;
```

When you have finished the lab, stop Hadoop services from the Ubuntu
shell:

``` bash
stop-yarn.sh
stop-dfs.sh
```

Next time, start SSH and Hadoop before launching Hive:

``` bash
sudo service ssh start
start-dfs.sh
start-yarn.sh
hive
```

Do not run `hdfs namenode -format` again after the first successful
initialization.

------------------------------------------------------------------------

## Result

**Hive was installed successfully, a database and table were created,
CSV data was loaded, and basic HiveQL queries were executed.**

## Troubleshooting

-   **`Table not found 'employees'`:** Run `USE mydatabase;` and
    `SHOW TABLES;`. Create the table before loading the CSV.
-   **CSV path does not exist:** Run `whoami` and
    `ls -l ~/employees.csv`; use the actual Linux username in the Hive
    path.
-   **`Connection refused` or HDFS errors:** Check `jps`; start SSH,
    HDFS, and YARN.
-   **Java/Hive `ClassCastException`:** Confirm Java 8 is installed and
    that both `JAVA_HOME` in `~/.bashrc` and `hadoop-env.sh` point to
    the Java 8 directory. Close and reopen Ubuntu or run
    `source ~/.bashrc`.
-   **Metastore schema already exists:** Do not initialize it again.
    Continue to the Hive launch step.
-   **Hadoop daemons fail after a Java change:** Stop the daemons,
    verify Java paths, then start them again. Avoid formatting the
    NameNode as a troubleshooting step.


## Experiment 6: Installation of HBase + Practice Examples

Continue in the same WSL Ubuntu terminal (Hadoop should still be running — check with `jps`).

### Step 1 — Download and install HBase
```bash
cd ~
wget https://downloads.apache.org/hbase/2.5.5/hbase-2.5.5-bin.tar.gz
tar -xzvf hbase-2.5.5-bin.tar.gz
sudo mv hbase-2.5.5 /usr/local/hbase
```

### Step 2 — Set environment variables
```bash
nano ~/.bashrc
```
Add:
```bash
export HBASE_HOME=/usr/local/hbase
export PATH=$PATH:$HBASE_HOME/bin
```
```bash
source ~/.bashrc
```

### Step 3 — Configure HBase (standalone mode — simplest, uses HDFS you already have running)
```bash
nano $HBASE_HOME/conf/hbase-site.xml
```
```xml
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/home/<your-username>/zookeeper</value>
  </property>
</configuration>
```
Also set JAVA_HOME in HBase's env file:
```bash
nano $HBASE_HOME/conf/hbase-env.sh
```
Find and set:
```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```

### Step 4 — Start HBase
```bash
start-hbase.sh
jps
```
You should now see `HMaster` and `HRegionServer` added to your `jps` output (alongside NameNode/DataNode).

### Step 5 — Launch the HBase shell and practice
```bash
hbase shell
```
```
create 'students', 'info'
list
put 'students', '1', 'info:name', 'John'
put 'students', '1', 'info:age', '21'
put 'students', '2', 'info:name', 'Alice'
put 'students', '2', 'info:age', '22'
scan 'students'
get 'students', '1'
disable 'students'
enable 'students'
```

**Result:** "HBase was installed successfully and basic table create/insert/scan operations were performed."

---

## Experiment 7: Installing Thrift

Apache Thrift lets services in different languages (e.g. Python, Java) talk to Hive/HBase. This experiment is just the install + version check.

### Step 1 — Install build dependencies
```bash
sudo apt update
sudo apt install -y automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config
```

### Step 2 — Easiest path: install via apt (pre-built package)
```bash
sudo apt install -y thrift-compiler
thrift --version
```
If that works, you're done — skip to Step 4.

### Step 3 — If apt doesn't have it (or you need a specific version), build from source
```bash
cd ~
wget https://dlcdn.apache.org/thrift/0.19.0/thrift-0.19.0.tar.gz
tar -xzvf thrift-0.19.0.tar.gz
cd thrift-0.19.0
./configure
make
sudo make install
thrift --version
```
(This build can take 10–15 minutes — that's normal.)

### Step 4 — Test it with a simple .thrift file
```bash
cd ~
nano hello.thrift
```
```thrift
namespace java com.example.hello

service HelloService {
    string sayHello(1: string name)
}
```
Generate Java bindings from it:
```bash
thrift --gen java hello.thrift
ls gen-java/com/example/hello/
```
You should see generated `.java` files like `HelloService.java` — this confirms Thrift is correctly installed and can generate code.

**Result:** "Apache Thrift was installed successfully and verified by generating service bindings from a sample .thrift file."

---

## Experiment 8: Practice Importing and Exporting Data from Various Databases

This experiment uses **Sqoop** (the standard Hadoop tool for moving data between HDFS/Hive and relational databases like MySQL).

### Step 1 — Install MySQL (as the "external database")
```bash
sudo apt install mysql-server -y
sudo service mysql start
sudo mysql
```
In the MySQL prompt:
```sql
CREATE DATABASE labdb;
USE labdb;
CREATE TABLE students (id INT, name VARCHAR(50), marks INT);
INSERT INTO students VALUES (1,'John',85),(2,'Alice',90),(3,'Bob',78);
CREATE USER 'hiveuser'@'localhost' IDENTIFIED BY 'hivepass';
GRANT ALL PRIVILEGES ON labdb.* TO 'hiveuser'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

### Step 2 — Install Sqoop
```bash
cd ~
wget https://downloads.apache.org/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
tar -xzvf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
sudo mv sqoop-1.4.7.bin__hadoop-2.6.0 /usr/local/sqoop
```
Set environment variables:
```bash
nano ~/.bashrc
```
Add:
```bash
export SQOOP_HOME=/usr/local/sqoop
export PATH=$PATH:$SQOOP_HOME/bin
```
```bash
source ~/.bashrc
```

### Step 3 — Add the MySQL JDBC connector (required)
```bash
cd ~
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar
cp mysql-connector-java-8.0.28.jar $SQOOP_HOME/lib/
```

### Step 4 — Import: MySQL → HDFS
```bash
sqoop import \
  --connect jdbc:mysql://localhost/labdb \
  --username hiveuser --password hivepass \
  --table students \
  --target-dir /sqoop_output/students \
  -m 1
```

### Step 5 — Verify the imported data
```bash
hdfs dfs -cat /sqoop_output/students/part-m-00000
```
Expected: the same 3 rows you inserted into MySQL, now as HDFS text.

### Step 6 — Export: HDFS → MySQL (reverse direction)
Create a new empty table to export into:
```bash
mysql -u hiveuser -phivepass labdb -e "CREATE TABLE students_export (id INT, name VARCHAR(50), marks INT);"
```
```bash
sqoop export \
  --connect jdbc:mysql://localhost/labdb \
  --username hiveuser --password hivepass \
  --table students_export \
  --export-dir /sqoop_output/students \
  -m 1
```

### Step 7 — Verify export
```bash
mysql -u hiveuser -phivepass labdb -e "SELECT * FROM students_export;"
```
You should see the 3 rows now present in `students_export`, confirming the round trip worked.

**Result:** "Data import and export between HDFS and a relational database (MySQL) was practiced successfully using Sqoop."

---

## Submission Checklist (2–8)
- [ ] Exp 2: screenshots of `-put`, `-ls`, `-get`, `-cat`, `-rm` outputs
- [ ] Exp 3: matrix multiplication Java code screenshots + `part-r-00000` output
- [ ] Exp 4: word count Java code screenshots + `part-r-00000` output
- [ ] Exp 5: `hive>` prompt + `SHOW DATABASES` + `SELECT * FROM employees` output
- [ ] Exp 6: `jps` showing HMaster/HRegionServer + `hbase shell` scan output
- [ ] Exp 7: `thrift --version` + generated `gen-java` files listing
- [ ] Exp 8: Sqoop import log + HDFS file content + Sqoop export + final MySQL SELECT

If any command errors out, paste the **exact error text** and tell me which experiment/step — I'll debug that specific one with you.
