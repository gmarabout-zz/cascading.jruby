# Copyright 2009, Grégoire Marabout. All Rights Reserved.
#
# This is free software. Please see the LICENSE and COPYING files for details.

require 'java'

module Cascading
  # :stopdoc:
  VERSION = '0.0.3'
  LIBPATH = ::File.expand_path(::File.dirname(__FILE__)) + ::File::SEPARATOR
  PATH = ::File.dirname(LIBPATH) + ::File::SEPARATOR
  CASCADING_HOME = ENV['CASCADING_HOME']
  HADOOP_HOME = ENV['HADOOP_HOME']

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

  def self.require_all_jars(from = ::File.join(::File.dirname(__FILE__), "..", "jars"))
    search_me = ::File.expand_path(
        ::File.join(from, '**', '*.jar'))
    Dir.glob(search_me).sort.each do |jar|
      #puts "required: #{jar}"
      require jar
    end
  end
end

Cascading.require_all_jars(Cascading::HADOOP_HOME) if Cascading::HADOOP_HOME
Cascading.require_all_jars(Cascading::CASCADING_HOME) if Cascading::CASCADING_HOME

require 'cascading/assembly'
require 'cascading/base'
require 'cascading/cascade'
require 'cascading/cascading'
require 'cascading/cascading_exception'
require 'cascading/expr_stub'
require 'cascading/flow'
require 'cascading/operations'
require 'cascading/scope'

# include module to make them available at top package
include Cascading
