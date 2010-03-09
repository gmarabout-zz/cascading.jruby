#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'samples/data/data2.txt'
output = 'output/rename'

Cascading::Flow.new('rename') do
  source 'extract', tap(input)

  assembly 'extract' do
    split 'line', ['name', 'score1', 'score2', 'id'], :output => ['name', 'score1', 'score2', 'id']
    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
    rename 'name' => 'new_name', 'score1' => 'new_score1', 'score2' => 'new_score2'
    assert Java::CascadingOperationAssertion::AssertSizeEquals.new(4)
    puts "Final field names: #{scope.values_fields.to_a.inspect}"
  end

  sink 'extract', tap(output, :sink_mode => :replace)
end.complete(sample_properties)
