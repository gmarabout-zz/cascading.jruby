require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Cascading::Scope do
  it 'should match Cascading fields names from source tap scheme' do
    test_assembly do
      # Pass that uses our scope instead of all_fields
      operation = Java::CascadingOperation::Identity.new 
      make_each(Java::CascadingPipe::Each, tail_pipe, scope.values_fields, operation)

      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should match Cascading fields names after CoGroup' do
    test_join_assembly do
      # Pass that uses our scope instead of all_fields
      operation = Java::CascadingOperation::Identity.new 
      make_each(Java::CascadingPipe::Each, tail_pipe, scope.values_fields, operation)

      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_']
    end
  end

  it 'should match Cascading fields names after Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int

      # Pass that uses our grouping fields instead of all_fields
      operation = Java::CascadingOperation::Identity.new 
      make_each(# FIXME: names of grouping fields are not what we'd expect!
                Java::CascadingPipe::Each, tail_pipe, fields([0, 'x_sum']), operation)

      check_scope :values_fields => [0, 'x_sum']
    end
  end

  it 'should pick up names from source tap scheme' do
    test_assembly do
      pass

      check_scope :values_fields => ['offset', 'line']
    end
  end

  it 'should propagate names through Each' do
    test_assembly do
      check_scope :values_fields => ['offset', 'line']
      assert_size_equals 2

      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y']
      assert_size_equals 4
    end
  end

  it 'should allow field filtration at Each' do
    test_assembly do
      check_scope :values_fields => ['offset', 'line']
      assert_size_equals 2

      split 'line', ['x', 'y'], :pattern => /,/, :output => ['x', 'y']
      check_scope :values_fields => ['x', 'y']
      assert_size_equals 2
    end
  end

  it 'should propagate names through CoGroup' do
    test_join_assembly do
    end
  end

  it 'should pass grouping fields to Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum']
      assert_group_size_equals 1
    end
  end

  it 'should pass grouping fields through chained Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum']
      assert_group_size_equals 1

      sum :mapping => {'y' => 'y_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum', 'y_sum']
      assert_group_size_equals 1
    end
  end

  it 'should propagate names through Every' do
    test_join_assembly do
      sum :mapping => {'x' => 'x_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum']
      assert_group_size_equals 1

      sum :mapping => {'y' => 'y_sum'}, :type => :int
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum', 'y_sum']
      assert_group_size_equals 1

      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x', 'x_sum', 'y_sum']
      assert_size_equals 3

      # No rename service provided unless you use the block form of join!
      check_scope :values_fields => [0, 'x_sum', 'y_sum']

      # Mimic rename service
      bind_names ['x', 'x_sum', 'y_sum']
      check_scope :values_fields => ['x', 'x_sum', 'y_sum']
    end
  end

  it 'should pass values fields to Each immediately following CoGroup and remove grouping fields' do
    test_join_assembly do
      assert_size_equals 10
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_']
    end
  end

  it 'should fail to pass grouping fields to Every immediately following Each' do
    lambda do # Composition fails
      test_join_assembly do
        pass
        check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_']
        begin
          sum :mapping => {'x' => 'x_sum'}, :type => :int
        rescue CascadingException => e
          raise e.cause(3)
        end
      end
    end.should raise_error java.lang.IllegalStateException, 'Every cannot follow a Tap or an Each'
  end

  it 'should propagate values fields and field names into branch' do
    test_join_assembly(:branches => ['data_tuple']) do
      branch 'data_tuple' do
        check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
          :grouping_fields => ['x']
        assert_size_equals 10
      end
    end
  end

  it 'should fail to propagate grouping fields to branch' do
    lambda do # Execution fails
      begin
        test_join_assembly(:branches => ['attempt_group']) do
          branch 'attempt_group' do
            check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
              :grouping_fields => ['x']
            sum :mapping => {'x' => 'x_sum'}, :type => :int
          end
        end
      rescue CascadingException => e
        raise e.cause(4)
      end
    end.should raise_error java.lang.IllegalStateException, 'Every cannot follow a Tap or an Each'
  end

  it 'should propagate names through GroupBy' do
    test_assembly do
      group_by 'line'
      check_scope :values_fields => ['offset', 'line'],
        :grouping_fields => ['line']
    end
  end
end
