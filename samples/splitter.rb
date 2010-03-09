#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'samples/data/data2.txt'

Cascading::Flow.new('copy') do
  source 'copy', tap(input)

  assembly 'copy' do
    split 'line', ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
    group_by 'score1' do
      count
    end
  end

  sink 'copy', tap('output/splitted', :sink_mode => :replace)
end.complete(sample_properties)
