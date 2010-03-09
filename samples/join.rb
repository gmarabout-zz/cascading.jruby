#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input1 = 'samples/data/data_join1.txt'
input2 = 'samples/data/data_join2.txt'
input3 = 'samples/data/data_join3.txt'
output = 'output/joined'

Cascading::Flow.new('join') do
  source 'extract1', tap(input1)
  source 'extract2', tap(input2)
  source 'extract3', tap(input3)

  assembly 'extract1' do
    split 'line', ['id', 'name']
  end

  assembly 'extract2' do
    split 'line', ['id', 'age']
  end

  assembly 'extract3' do
    split 'line', ['id', 'city']
  end

  assembly 'join' do
    join 'extract1', 'extract2', 'extract3', :on => 'id'
    project 'id', 'name', 'age', 'city'
  end

  sink 'join', tap(output, :sink_mode => :replace)
end.complete(sample_properties)
