#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'output/fetched/to_be_copied.txt'
dataUrl = 'http://www.census.gov/genealogy/names/dist.all.last'
system "curl --create-dirs -o #{input} #{dataUrl}" unless File.exists?(input)

cascade 'copy' do
  flow 'copy' do
    source 'input', tap(input)

    assembly 'input' do
      rename 'line' => 'value'
      reject 'value:string.indexOf("R") == -1'
    end

    sink 'input', tap('output/copied', :sink_mode => :replace)
  end
end.complete(sample_properties)
