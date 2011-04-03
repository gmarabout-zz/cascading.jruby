#! /usr/bin/env jruby
$: << File.join(File.dirname(__FILE__), '..', 'lib')

require 'cascading'
require 'samples/cascading'

input = 'output/fetched/fetch.txt'
dataUrl = 'http://www.gutenberg.org/files/20417/20417-8.txt'
system "curl --create-dirs -o #{input} #{dataUrl}" unless File.exists?(input)

cascade 'logwordcount' do
  flow 'logwordcount' do
    source 'input', tap(input)

    assembly 'input' do
      # TODO: create a helper for RegexSplitGenerator
      each 'line', :function => regex_split_generator('word', :pattern => /[.,]*\s+/)
      group_by 'word' do
        count
      end
      group_by 'count', :reverse => true
    end

    sink 'input', tap('output/imported', :sink_mode => :replace)
  end
end.complete(sample_properties)
