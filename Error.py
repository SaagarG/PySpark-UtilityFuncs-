{
	"name": "Py4JJavaError",
	"message": "An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
: Status code: -1 error code: null error message: Auth failure: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator$HttpException: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: 
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.executeHttpOperation(AbfsRestOperation.java:274)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.completeExecute(AbfsRestOperation.java:217)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.lambda$execute$0(AbfsRestOperation.java:191)
\tat org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation(IOStatisticsBinding.java:464)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.execute(AbfsRestOperation.java:189)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAclStatus(AbfsClient.java:911)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAclStatus(AbfsClient.java:892)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getIsNamespaceEnabled(AzureBlobFileSystemStore.java:416)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getFileStatus(AzureBlobFileSystemStore.java:1029)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.getFileStatus(AzureBlobFileSystem.java:645)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.getFileStatus(AzureBlobFileSystem.java:635)
\tat org.apache.spark.deploy.history.EventLogFileWriter.requireLogBaseDirAsDirectory(EventLogFileWriters.scala:77)
\tat org.apache.spark.deploy.history.SingleEventLogFileWriter.start(EventLogFileWriters.scala:228)
\tat org.apache.spark.scheduler.EventLoggingListener.start(EventLoggingListener.scala:93)
\tat org.apache.spark.SparkContext.<init>(SparkContext.scala:667)
\tat org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:58)
\tat sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
\tat sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
\tat sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
\tat java.lang.reflect.Constructor.newInstance(Constructor.java:423)
\tat py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:247)
\tat py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
\tat py4j.Gateway.invoke(Gateway.java:238)
\tat py4j.commands.ConstructorCommand.invokeConstructor(ConstructorCommand.java:80)
\tat py4j.commands.ConstructorCommand.execute(ConstructorCommand.java:69)
\tat py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
\tat py4j.ClientServerConnection.run(ClientServerConnection.java:106)
\tat java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator$HttpException: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: 
\tat org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter.refreshToken(CustomTokenProviderAdapter.java:91)
\tat org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider.getToken(AccessTokenProvider.java:50)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAccessToken(AbfsClient.java:1055)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.executeHttpOperation(AbfsRestOperation.java:256)
\t... 27 more
",
	"stack": "---------------------------------------------------------------------------
Py4JJavaError                             Traceback (most recent call last)
<ipython-input-3-685099a61623> in <module>
     34     SparkContext._active_spark_context.stop()
     35 
---> 36 spark = SparkSession.builder.config(conf=conf)\\
     37                             .enableHiveSupport().getOrCreate()
     38 

/usr/hdp/current/spark3-client/python/pyspark/sql/session.py in getOrCreate(self)
    267                         sparkConf.set(key, value)
    268                     # This SparkContext may be an existing one.
--> 269                     sc = SparkContext.getOrCreate(sparkConf)
    270                     # Do not update `SparkConf` for existing `SparkContext`, as it's shared
    271                     # by all sessions.

/usr/hdp/current/spark3-client/python/pyspark/context.py in getOrCreate(cls, conf)
    482         with SparkContext._lock:
    483             if SparkContext._active_spark_context is None:
--> 484                 SparkContext(conf=conf or SparkConf())
    485             assert SparkContext._active_spark_context is not None
    486             return SparkContext._active_spark_context

/usr/hdp/current/spark3-client/python/pyspark/context.py in __init__(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc, profiler_cls, udf_profiler_cls)
    208                 jsc,
    209                 profiler_cls,
--> 210                 udf_profiler_cls,
    211             )
    212         except BaseException:

/usr/hdp/current/spark3-client/python/pyspark/context.py in _do_init(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, jsc, profiler_cls, udf_profiler_cls)
    281 
    282         # Create the Java SparkContext through Py4J
--> 283         self._jsc = jsc or self._initialize_context(self._conf._jconf)
    284         # Reset the SparkConf to the one actually used by the SparkContext in JVM.
    285         self._conf = SparkConf(_jconf=self._jsc.sc().conf())

/usr/hdp/current/spark3-client/python/pyspark/context.py in _initialize_context(self, jconf)
    401         \"\"\"
    402         assert self._jvm is not None
--> 403         return self._jvm.JavaSparkContext(jconf)
    404 
    405     @classmethod

/usr/hdp/current/spark3-client/python/lib/py4j-0.10.9.5-src.zip/py4j/java_gateway.py in __call__(self, *args)
   1584         answer = self._gateway_client.send_command(command)
   1585         return_value = get_return_value(
-> 1586             answer, self._gateway_client, None, self._fqn)
   1587 
   1588         for temp_arg in temp_args:

/usr/hdp/current/spark3-client/python/lib/py4j-0.10.9.5-src.zip/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
    326                 raise Py4JJavaError(
    327                     \"An error occurred while calling {0}{1}{2}.\
\".
--> 328                     format(target_id, \".\", name), value)
    329             else:
    330                 raise Py4JError(

Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
: Status code: -1 error code: null error message: Auth failure: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator$HttpException: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: 
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.executeHttpOperation(AbfsRestOperation.java:274)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.completeExecute(AbfsRestOperation.java:217)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.lambda$execute$0(AbfsRestOperation.java:191)
\tat org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation(IOStatisticsBinding.java:464)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.execute(AbfsRestOperation.java:189)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAclStatus(AbfsClient.java:911)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAclStatus(AbfsClient.java:892)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getIsNamespaceEnabled(AzureBlobFileSystemStore.java:416)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore.getFileStatus(AzureBlobFileSystemStore.java:1029)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.getFileStatus(AzureBlobFileSystem.java:645)
\tat org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem.getFileStatus(AzureBlobFileSystem.java:635)
\tat org.apache.spark.deploy.history.EventLogFileWriter.requireLogBaseDirAsDirectory(EventLogFileWriters.scala:77)
\tat org.apache.spark.deploy.history.SingleEventLogFileWriter.start(EventLogFileWriters.scala:228)
\tat org.apache.spark.scheduler.EventLoggingListener.start(EventLoggingListener.scala:93)
\tat org.apache.spark.SparkContext.<init>(SparkContext.scala:667)
\tat org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:58)
\tat sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
\tat sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
\tat sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
\tat java.lang.reflect.Constructor.newInstance(Constructor.java:423)
\tat py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:247)
\tat py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
\tat py4j.Gateway.invoke(Gateway.java:238)
\tat py4j.commands.ConstructorCommand.invokeConstructor(ConstructorCommand.java:80)
\tat py4j.commands.ConstructorCommand.execute(ConstructorCommand.java:69)
\tat py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
\tat py4j.ClientServerConnection.run(ClientServerConnection.java:106)
\tat java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator$HttpException: HTTP Error -1CustomTokenProvider getAccessToken threw java.io.IOException : Authentication Failed: 
\tat org.apache.hadoop.fs.azurebfs.oauth2.CustomTokenProviderAdapter.refreshToken(CustomTokenProviderAdapter.java:91)
\tat org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider.getToken(AccessTokenProvider.java:50)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsClient.getAccessToken(AbfsClient.java:1055)
\tat org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation.executeHttpOperation(AbfsRestOperation.java:256)
\t... 27 more
"
}
