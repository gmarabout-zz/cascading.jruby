#! /usr/bin/env jruby

$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

cascade 'branch' do
  flow 'branch' do
    source 'input', tap('samples/data/data2.txt')

    assembly 'input' do
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

    sink 'branch1', tap('output/branch1', :sink_mode => :replace)
    sink 'branch2', tap('output/branch2', :sink_mode => :replace)
  end
end.complete(sample_properties)
