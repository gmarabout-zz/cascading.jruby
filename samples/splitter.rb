#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'samples/data/data2.txt'

cascade 'splitter' do
  flow 'splitter' do
    source 'input', tap(input)

    assembly 'input' do
      split 'line', ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
      group_by 'score1' do
        count
      end
    end

    sink 'input', tap('output/splitted', :sink_mode => :replace)
  end
end.complete(sample_properties)
