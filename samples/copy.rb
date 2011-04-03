#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

cascade 'copy' do
  flow 'copy' do
    source 'input', tap('http://www.census.gov/genealogy/names/dist.all.last')

    assembly 'input' do
      rename 'line' => 'value'
      reject 'value:string.indexOf("R") == -1'
    end

    sink 'input', tap('output/copy', :sink_mode => :replace)
  end
end.complete(sample_properties)
