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
