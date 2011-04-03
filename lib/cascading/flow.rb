# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'cascading/assembly'

module Cascading
  class Flow < Cascading::Node
    extend Registerable

    attr_accessor :properties, :sources, :sinks, :outgoing_scopes, :listeners

    def initialize(name, parent, &block)
      @properties, @sources, @sinks, @outgoing_scopes, @listeners = {}, {}, {}, {}, []
      self.class.add(name, self)
      super(name, parent, &block)
    end

    def assembly(name, &block)
      raise "Could not build assembly '#{name}'; block required" unless block_given?
      add_child(Assembly.new(name, self, @outgoing_scopes, &block))
    end

    # Create a new sink for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will
    # reference a path.
    def sink(*args)
      if (args.size == 2)
        @sinks[args[0]] = args[1]
      elsif (args.size == 1)
        @sinks[@name] =  args[0]
      end
    end

    # Create a new source for this flow, with the specified name.
    # "tap" can be either a tap (see Cascading.tap) or a string that will
    # reference a path.
    def source(*args)
      if (args.size == 2)
        @sources[args[0]] = args[1]
        @outgoing_scopes[args[0]] = Scope.tap_scope(args[1], args[0])
      elsif (args.size == 1)
        @sources[@name] = args[0]
        @outgoing_scopes[@name] = Scope.empty_scope(@name)
      end
    end

    def scope(name = nil)
      raise 'Must specify name if no children have been defined yet' unless name || last_child
      name ||= last_child.name
      @outgoing_scopes[name]
    end

    def debug_scope(name = nil)
      scope = scope(name)
      name ||= last_child.name
      puts "Scope for '#{name}':\n  #{scope}"
    end

    def sink_metadata
      @sinks.keys.inject({}) do |sink_metadata, sink_name|
        raise "Cannot sink undefined assembly '#{sink_name}'" unless @outgoing_scopes[sink_name]
        sink_metadata[sink_name] = {
          :field_names => @outgoing_scopes[sink_name].values_fields.to_a,
          :primary_key => @outgoing_scopes[sink_name].primary_key_fields.to_a
        }
        sink_metadata
      end
    end

    # TODO: support all codecs, support list of codecs
    def compress_output(codec, type)
      properties['mapred.output.compress'] = 'true'
      properties['mapred.output.compression.codec'] = case codec
        when :default then Java::OrgApacheHadoopIoCompress::DefaultCodec.java_class.name
        when :gzip then Java::OrgApacheHadoopIoCompress::GzipCodec.java_class.name
        else raise "Codec #{codec} not yet supported by cascading.jruby"
        end
      properties['mapred.output.compression.type'] = case type
        when :none   then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::NONE.to_s
        when :record then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::RECORD.to_s
        when :block  then Java::OrgApacheHadoopIo::SequenceFile::CompressionType::BLOCK.to_s
        else raise "Compression type '#{type}' not supported"
        end
    end

    def set_spill_threshold(threshold)
      properties['cascading.cogroup.spill.threshold'] = threshold.to_s
    end

    def add_file_to_distributed_cache(file)
      add_to_distributed_cache(file, "mapred.cache.files")
    end

    def add_archive_to_distributed_cache(file)
      add_to_distributed_cache(file, "mapred.cache.archives")
    end

    def add_listener(listener)
      @listeners << listener
    end

    def emr_local_path_for_distributed_cache_file(file)
      # NOTE this needs to be *appended* to the property mapred.local.dir
      if file =~ /^s3n?:\/\//
        # EMR
        "/taskTracker/archive/#{file.gsub(/^s3n?:\/\//, "")}"
      else
        # Local
        file
      end
    end

    def add_to_distributed_cache(file, property)
      v = properties[property]

      if v
        properties[property] = [v.split(/,/), file].flatten.join(",")
      else
        properties[property] = file
      end
    end


    def connect(properties = nil)
      properties ||= java.util.HashMap.new(@properties)
      parameters = build_connect_parameter()
      Java::CascadingFlow::FlowConnector.new(properties).connect(*parameters)
    end

    def complete(properties = nil)
      begin
        flow = connect(properties)
        @listeners.each { |l| flow.addListener(l) }
        flow.complete
      rescue NativeException => e
        raise CascadingException.new(e, 'Error completing flow')
      end
    end

    private

    def build_connect_parameter
      sources = make_tap_parameter(@sources)
      sinks = make_tap_parameter(@sinks)
      pipes = make_pipes
      [sources, sinks, pipes]
    end

    def make_tap_parameter(taps)
      taps.inject({}) do |map, (name, tap)|
        assembly = find_child(name)
        raise "Could not find assembly '#{name}' to connect to tap: #{tap}" unless assembly

        map[assembly.tail_pipe.name] = tap
        map
      end
    end

    def make_pipes
      @sinks.inject([]) do |pipes, (name, sink)|
        assembly = find_child(name)
        raise "Could not find assembly '#{name}' to make pipe for sink: #{sink}" unless assembly
        pipes << assembly.tail_pipe
        pipes
      end.to_java(Java::CascadingPipe::Pipe)
    end
  end
end
