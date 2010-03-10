require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Cascading do
  it 'should dedup field names from multiple sources' do
    left_names = ['a', 'b', 'c', 'd', 'e']
    mid_names = ['a', 'f']
    right_names = ['a', 'g']

    field_names = dedup_field_names(left_names, mid_names, right_names)
    field_names.should == [
      'a', 'b', 'c', 'd', 'e',
      'a_', 'f',
      'a__', 'g'
    ]
  end

  it 'should fail to resolve duplicate fields' do
    incoming = fields(['line'])
    declared = fields(['line'])
    outgoing = all_fields
    lambda do
      begin
        resolved = Java::CascadingTuple::Fields.resolve(outgoing, [incoming, declared].to_java(Java::CascadingTuple::Fields))
      rescue NativeException => e
        raise e.cause
      end
    end.should raise_error Java::CascadingTuple::TupleException, 'field name already exists: line'
  end
end
