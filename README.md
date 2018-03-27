# spark-hbase-mock
Hbase mock testing using spark scala test


Running Hbase Testing Utility On Windows

The HBase Testing Utility is a vital tool for anyone writing HBase applications. 
It sets up (and tears down) a lightweight HBase instance locally to allow local integration tests. 
I’ve previously discussed this and how to use it with BDD Cucumber tests in this blog post, complete with working repo. 
However, it is not trivially easy to get working on Windows machines, nor is it documented anywhere.

In this blog post, I’ll show how to get the HBase Testing Utility running on Windows machines, no admin access required. 
There’s no accompanying GitHub project for this post, as it’s fairly short and generic. 
I’ll assume you already have a working HBase Testing Utility test that runs on Unix, and you want to port it to Windows.

Download the https://github.com/sardetushar/hadooponwindows

Download/clone Winutils. This contains several Hadoop versions compiled for Windows.
Go to Control Panel, and find Edit environment variables for your account in System.
Add the following user variables:
hadoop.home.dir=<PATH_TO_DESIRED_HADOOP_VERSION> (in my case, this was C:\Users\bwatson\apps\hadoop-2.8.3)
HADOOP_HOME=<PATH_TO_DESIRED_HADOOP_VERSION> (as above)
append %HADOOP_HOME%/bin to Path

Before calling new HBaseTestingUtility(); 
the temporary HBase data directory needs to be set. 
Add System.setProperty("test.build.data.basedirectory", "C:/Temp/hbase"); to the code. 
This path can be changed, but it’s important to keep it short. Using the JUnit TemporaryFolder or the default path results in paths 
too long, and shows an error similar to
java.io.IOException: Failed to move meta file for ReplicaBeingWritten, blk_1073741825_1001, RBW.
