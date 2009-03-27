# encoding: utf-8
 
# cascading.rb : A DSL library for Cascading, in JRuby.
#
# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'java'

module Cascading

  # :stopdoc:
  VERSION = '0.0.2'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH = ::File.dirname(LIBPATH) + ::File::SEPARATOR
  
  CASCADING_HOME = ENV["CASCADING_HOME"]
  HADOOP_HOME = ENV["HADOOP_HOME"]
  
  # :startdoc:

  # Returns the version string for the library.
  #
  def self.version
    VERSION
  end

  # Returns the library path for the module. If any arguments are given,
  # they will be joined to the end of the libray path using
  # <tt>File.join</tt>.
  #
  def self.libpath( *args )
    args.empty? ? LIBPATH : ::File.join(LIBPATH, args.flatten)
  end

  # Returns the lpath for the module. If any arguments are given,
  # they will be joined to the end of the path using
  # <tt>File.join</tt>.
  #
  def self.path( *args )
    args.empty? ? PATH : ::File.join(PATH, args.flatten)
  end

  # Utility method used to require all files ending in .rb that lie in the
  # directory below this file that has the same name as the filename passed
  # in. Optionally, a specific _directory_ name can be passed in such that
  # the _filename_ does not have to be equivalent to the directory.
  #
  def self.require_all_libs_relative_to( fname, dir = nil )
    dir ||= ::File.basename(fname, '.*')
    search_me = ::File.expand_path(
        ::File.join(::File.dirname(fname), dir, '**', '*.rb'))
      
    Dir.glob(search_me).sort.each do |rb| 
      #puts "required: #{rb}"
      require rb
    end
  end
  
  def self.require_all_jars(from = ::File.join(::File.dirname(__FILE__), "..", "jars"))
    search_me = ::File.expand_path(
        ::File.join(from, '**', '*.jar'))
    Dir.glob(search_me).sort.each do |jar| 
      #puts "required: #{jar}"
      require jar
    end
  end

end  # module Cascading

Cascading.require_all_libs_relative_to(__FILE__)

if Cascading::HADOOP_HOME
  Cascading.require_all_jars(Cascading::HADOOP_HOME)
end

if Cascading::CASCADING_HOME  
  Cascading.require_all_jars(Cascading::CASCADING_HOME)
end


# include module to make them available at top package
include Cascading