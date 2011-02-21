# flow.rb
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require "cascading/assembly"

module Cascading
  class FlowFactory
    def assembly(node, *args, &block)
      name = args[0]
      if block
        Cascading::Assembly.new(name, nil, node.outgoing_scopes, &block)
      else
        Cascading::Assembly.get(name)
      end
    end
  end


  class Flow < Cascading::Node
    attr_accessor :properties, :sources, :sinks, :outgoing_scopes, :listeners

    def initialize(name, parent=nil, &block)
      @properties, @sources, @sinks, @outgoing_scopes = {}, {}, {}, {}
      @listeners = []
      super(name, parent, &block)
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
      raise 'Must specify name if no children have been defined yet' unless name || @children.last
      name ||= @children.last.name
      @outgoing_scopes[name]
    end

    def debug_scope(name = nil)
      scope = scope(name)
      name ||= @children.last.name
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

    def make_tap_parameter taps
      taps.keys.inject({}) do |map, key|
        assembly = Assembly.get(key)

        if assembly
          map[assembly.tail_pipe.name] = taps[key]
        else
          map[key] = taps[key]
        end

        map
      end
    end

    def make_pipes
      pipes = []
      @sinks.keys.each do |key|
        assembly = Assembly.get(key)
        raise "Undefined assembly #{key}" unless assembly
        pipes << assembly.tail_pipe
      end
      return pipes.to_java(Java::CascadingPipe::Pipe)
    end
  end
end
