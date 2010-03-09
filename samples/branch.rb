#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'samples/data/data2.txt'
output1, output2 = 'output/branch1', 'output/branch2'

Cascading::Flow.new('copy_to_mysql') do
  source 'extract', tap(input)

  assembly 'extract' do
    split 'line', ['name', 'score1', 'score2', 'id'], :pattern => /[.,]*\s+/

    branch 'branch1' do
      group_by 'score1' do
        count
      end
    end

    branch 'branch2' do
      group_by 'score2' do
        count
      end
    end
  end

  sink 'branch1', tap(output1, :sink_mode => :replace)
  sink 'branch2', tap(output2, :sink_mode => :replace)
end.complete(sample_properties)
