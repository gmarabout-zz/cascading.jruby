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

  it 'should find branches to sink' do
    cascade 'branched_pass' do
      flow 'branched_pass' do
        source 'input', tap('spec/resource/test_input.txt', :kind => :lfs, :scheme => text_line_scheme)
        assembly 'input' do
          branch 'branched_input' do
            project 'line'
          end
        end
        sink 'branched_input', tap("#{OUTPUT_DIR}/branched_pass_out", :kind => :lfs, :sink_mode => :replace)
      end
    end.complete

    ilc = `wc -l spec/resource/test_input.txt`.split(/\s+/).first
    olc = `wc -l #{OUTPUT_DIR}/branched_pass_out/part-00000`.split(/\s+/).first
    ilc.should == olc
  end

  it 'should create an isolated namespace per cascade' do
    cascade 'double' do
      flow 'double' do
        source 'input', tap('spec/resource/test_input.txt', :kind => :lfs, :scheme => text_line_scheme)
        assembly 'input' do # Dup name
          insert 'doubled' => expr('line:string + "," + line:string')
          project 'doubled'
        end
        sink 'input', tap("#{OUTPUT_DIR}/double_out", :kind => :lfs, :sink_mode => :replace)
      end
    end

    cascade 'pass' do
      flow 'pass' do
        source 'input', tap('spec/resource/test_input.txt', :kind => :lfs, :scheme => text_line_scheme)
        assembly 'input' do # Dup name
          project 'line'
        end
        sink 'input', tap("#{OUTPUT_DIR}/pass_out", :kind => :lfs, :sink_mode => :replace)
      end
    end

    Cascade.get('double').complete
    Cascade.get('pass').complete
    diff = `diff #{OUTPUT_DIR}/double_out/part-00000 #{OUTPUT_DIR}/pass_out/part-00000`
    diff.should_not be_empty
  end
end
