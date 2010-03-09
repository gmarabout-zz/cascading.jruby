module Cascading
  # Constructs properties to be passed to Flow#complete or Cascade#complete
  # which will locate temporary Hadoop files in build/sample.  It is necessary
  # to pass these properties only because the sample apps are invoked using
  # JRuby's main method, which confuses the JobConf's attempt to find the
  # containing jar.
  def sample_properties
    build_dir = 'build/sample/build'
    `mkdir -p #{build_dir}`
    tmp_dir = "build/sample/tmp"
    `mkdir -p #{tmp_dir}`
    log_dir = "build/sample/log"
    `mkdir -p #{log_dir}`

    # Local cluster settings
    #java.lang.System.set_property("test.build.data", build_dir)
    #java.lang.System.set_property("hadoop.tmp.dir", tmp_dir)
    #java.lang.System.set_property("hadoop.log.dir", log_dir)
    #conf = Java::OrgApacheHadoopConf::Configuration.new
    #dfs = Java::OrgApacheHadoopDfs::MiniDFSCluster.new(conf, 4, true, nil);
    #file_sys = dfs.file_system
    #mr = Java::OrgApacheHadoopMapred::MiniMRCluster.new(4, file_sys.uri.to_string, 1)
    #job_conf = mr.create_job_conf
    #job_conf.set("mapred.child.java.opts", "-Xmx512m")
    #job_conf.set("mapred.map.tasks.speculative.execution", "false")
    #job_conf.set("mapred.reduce.tasks.speculative.execution", "false")

    job_conf = Java::OrgApacheHadoopMapred::JobConf.new
    job_conf.jar = build_dir
    job_conf.set("test.build.data", build_dir)
    job_conf.set("hadoop.tmp.dir", tmp_dir)
    job_conf.set("hadoop.log.dir", log_dir)

    job_conf.num_map_tasks = 4
    job_conf.num_reduce_tasks = 1

    properties = java.util.HashMap.new({})
    Java::CascadingFlow::MultiMapReducePlanner.set_job_conf(properties, job_conf)
    properties
  end
end
