require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Cascading::Scope do
  it 'should allow override of primary key' do
    test_assembly do
      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
    end
  end

  it 'should pass primary key through Each' do
    test_assembly do
      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
      pass
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
    end
  end

  it 'should support renaming primary keys' do
    test_assembly do
      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
      rename 'offset' => 'primary_key', 'line' => 'data'
      check_scope :values_fields => ['primary_key', 'data', 'x', 'y'],
        :primary_key_fields => ['primary_key']
    end
  end

  it 'should clear primary keys when a subset of their fields are discarded' do
    test_assembly do
      primary 'offset', 'line' # Make primary keys interesting
      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset', 'line']
      project 'line', 'x', 'y'
      check_scope :values_fields => ['line', 'x', 'y'],
        :primary_key_fields => nil
    end
  end

  it 'should pass primary key through branch' do
    test_assembly do
      split 'line', ['x', 'y'], :pattern => /,/
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']

      branch 'check_keys' do
        check_scope :values_fields => ['offset', 'line', 'x', 'y'],
          :primary_key_fields => ['offset']
        pass
        check_scope :values_fields => ['offset', 'line', 'x', 'y'],
          :primary_key_fields => ['offset']
      end

      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
      pass
      check_scope :values_fields => ['offset', 'line', 'x', 'y'],
        :primary_key_fields => ['offset']
    end
  end

  it 'should pass primary key through GroupBy followed by Each' do
    test_assembly do
      group_by 'offset'
      check_scope :values_fields => ['offset', 'line'],
        :grouping_fields => ['offset'],
        :primary_key_fields => ['offset']
    end
  end

  it 'should pass primary key through GroupBy followed by Every' do
    test_assembly do
      split 'line', ['x', 'y'], :pattern => /,/
      group_by 'offset', 'x' do
        check_scope :values_fields => ['offset', 'line', 'x', 'y'],
          :grouping_fields => ['offset', 'x'],
          :primary_key_fields => ['offset'],
          :grouping_primary_key_fields => ['offset', 'x']
        count 'line'
        check_scope :values_fields => ['offset', 'line', 'x', 'y'],
          :grouping_fields => ['offset', 'x', 'line'],
          :primary_key_fields => ['offset'],
          :grouping_primary_key_fields => ['offset', 'x']
        count 'y'
        check_scope :values_fields => ['offset', 'line', 'x', 'y'],
          :grouping_fields => ['offset', 'x', 'line', 'y'],
          :primary_key_fields => ['offset', 'x'], # FIXME: why has the pk changed?
          :grouping_primary_key_fields => ['offset', 'x']
      end
      check_scope :values_fields => ['offset', 'x', 'line', 'y'],
        :primary_key_fields => ['offset', 'x']
    end
  end

  it 'should not clear primary key when grouping on other fields' do
    test_assembly do
      group_by 'line'
      check_scope :values_fields => ['offset', 'line'],
        :grouping_fields => ['line'],
        :primary_key_fields => ['offset'],
        :grouping_primary_key_fields => ['line']
    end
  end

  it 'should pass primary key through CoGroup' do
    test_join_assembly do
      check_scope :values_fields => ['offset', 'line', 'x', 'y', 'z', 'offset_', 'line_', 'x_', 'y_', 'z_'],
        :grouping_fields => ['x'],
        :primary_key_fields => ['offset'],
        :grouping_primary_key_fields => ['x']
    end
  end
end 
