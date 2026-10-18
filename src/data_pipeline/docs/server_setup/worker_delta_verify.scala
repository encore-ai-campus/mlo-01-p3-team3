// Isolated single-driver compatibility test. Never a production writer takeover.
try {
  import org.apache.hadoop.fs.{FileSystem, Path => HPath}
  import java.net.URI
  import io.delta.tables.DeltaTable
  val hosts = sc.parallelize(1 to 3,3).barrier().mapPartitions { it =>
    it.toList
    org.apache.spark.BarrierTaskContext.get().barrier()
    Iterator(java.net.InetAddress.getByName(org.apache.spark.SparkEnv.get.blockManager.blockManagerId.host).getHostAddress)
  }.collect().toSet
  println("PIPELINE_WORKER_HOSTS="+hosts.toSeq.sorted.mkString(","))
  require(hosts == Set("192.168.0.122","192.168.0.123","192.168.0.124"), "THREE_WORKERS_NOT_CONFIRMED")
  val run = sys.env("PJ3_GARAGE_DELTA_RUN")
  require(run.matches("pipeline-worker-delta-[a-f0-9]{32}"), "INVALID_TEST_RUN")
  val prefix = "s3a://pj3-data/verification/" + run
  val table = prefix + "/table"
  val props = new java.util.Properties()
  val input = new java.io.FileInputStream("/etc/pj3-clients/s3/spark.properties")
  try props.load(input) finally input.close()
  val hc = spark.sparkContext.hadoopConfiguration
  hc.set("fs.s3a.access.key", props.getProperty("accessKey"))
  hc.set("fs.s3a.secret.key", props.getProperty("secretKey"))
  val fs = FileSystem.get(new URI(prefix), hc)
  val root = new HPath(prefix)
  require(!fs.exists(root), "TEST_PREFIX_PREEXISTED")
  var owned = false
  var currentCount = 0L
  var fixedCount = 0L
  try {
    // Ownership is by a fresh private run prefix, used by this process only.
    owned = true
    spark.range(1).repartition(3).write.format("delta").mode("errorifexists").save(table)
    DeltaTable.forPath(spark,table).as("t")
      .merge(spark.range(1,2).toDF("id").as("s"), "t.id = s.id")
      .whenNotMatched().insertAll().execute()
    currentCount = spark.read.format("delta").load(table).count()
    fixedCount = spark.read.format("delta").option("versionAsOf",0).load(table).count()
    require(currentCount == 2, "CURRENT_COUNT")
    require(fixedCount == 1, "FIXED_VERSION_COUNT")
    val reopened = spark.newSession()
    require(reopened.read.format("delta").load(table).count() == 2, "NEW_SESSION_READ")
    println("PJ3_GARAGE_DELTA_PASS")
  } finally {
    if (owned) {
      require(fs.delete(root,true), "TEST_CLEANUP_FAILED")
      require(!fs.exists(root), "TEST_CLEANUP_NOT_CONFIRMED")
      println("PJ3_GARAGE_DELTA_CLEANED")
    }
  }
  val proof = s"""{"status":"VERIFIED","run_id":"$run","worker_hosts":["192.168.0.122","192.168.0.123","192.168.0.124"],"current_rows":$currentCount,"version_zero_rows":$fixedCount,"test_prefix_deleted":true}"""
  val proofPath = java.nio.file.Paths.get("delta-proof.json")
  java.nio.file.Files.writeString(proofPath,proof,java.nio.file.StandardOpenOption.CREATE_NEW)
  java.nio.file.Files.setPosixFilePermissions(proofPath,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"))
  spark.stop()
  System.exit(0)
} catch {
  case e: Throwable =>
    System.err.println("PJ3_GARAGE_DELTA_ERROR=" + e.getClass.getName)
    var cause: Throwable = e
    var depth = 0
    while (cause != null && depth < 5) {
      val message = Option(cause.getMessage).getOrElse("")
      if ((cause.isInstanceOf[NoClassDefFoundError] || cause.isInstanceOf[ClassNotFoundException]) &&
          message.matches("[A-Za-z0-9_.$/]+"))
        System.err.println("PJ3_GARAGE_DELTA_MISSING=" + message)
      cause = cause.getCause
      depth += 1
    }
    spark.stop()
    System.exit(1)
}
