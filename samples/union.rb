#! /usr/bin/env jruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'output/fetched/to_be_branched.txt'
dataUrl = 'http://www.census.gov/genealogy/names/dist.all.last'
system "curl --create-dirs -o #{input} #{dataUrl}" unless File.exists?(input)

output = 'output/union'

Cascading::Flow.new('copy_to_mysql') do
  source 'extract', tap(input)

  assembly 'extract' do
    split 'line', ['name', 'score1', 'score2', 'id']

    branch 'branch1' do
      group_by 'score1', 'name' do
        count
      end
      rename 'score1' => 'score'
    end

    branch 'branch2' do
      group_by 'score2', 'name' do
        count
      end
      rename 'score2' => 'score'
    end
  end

  assembly 'union' do
    union 'branch1', 'branch2'
  end

  sink 'union', tap(output, :sink_mode => :replace)
end.complete(sample_properties)
