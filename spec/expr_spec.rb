require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Object do
  it 'should allow expr syntax' do
    test_assembly do
      insert 'foo' => 1, 'bar' => expr('offset:int')
      check_scope :values_fields => ['offset', 'line', 'bar', 'foo']
    end
  end
end
