#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

cascade 'join' do
  flow 'join' do
    source 'input1', tap('samples/data/data_join1.txt')
    source 'input2', tap('samples/data/data_join2.txt')
    source 'input3', tap('samples/data/data_join3.txt')

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

    sink 'join', tap('output/join', :sink_mode => :replace)
  end
end.complete(sample_properties)
