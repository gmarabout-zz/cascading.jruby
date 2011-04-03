#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input1 = 'samples/data/data_join1.txt'
input2 = 'samples/data/data_join2.txt'
input3 = 'samples/data/data_join3.txt'
output = 'output/joined'

cascade 'join' do
  flow 'join' do
    source 'input1', tap(input1)
    source 'input2', tap(input2)
    source 'input3', tap(input3)

    assembly 'input1' do
      split 'line', ['id', 'name']
    end

    assembly 'input2' do
      split 'line', ['id', 'age']
    end

    assembly 'input3' do
      split 'line', ['id', 'city']
    end

    assembly 'join' do
      join 'input1', 'input2', 'input3', :on => 'id'
      project 'id', 'name', 'age', 'city'
    end

    sink 'join', tap(output, :sink_mode => :replace)
  end
end.complete(sample_properties)
