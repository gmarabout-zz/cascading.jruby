#! /usr/bin/ruby

require 'test/unit'
require 'cascading/assembly'


class TC_Assembly < Test::Unit::TestCase

  include Cascading::Operations

  def test_create_assembly_simple
    assemblyFactory = Cascading::AssemblyFactory.new("assembly1") do
    end

    assert_not_nil Cascading::AssemblyFactory.get("assembly1")
    assert_equal assemblyFactory, Cascading::AssemblyFactory.get("assembly1")
    pipe = assemblyFactory.make[0]
    assert pipe.is_a? Java::CascadingPipe::Pipe
  end

  def test_each_identity
    assemblyFactory = Cascading::AssemblyFactory.new("assembly1") do
      each "field1", :filter => identity
    end

    assert_not_nil Cascading::AssemblyFactory.get("assembly1")
    assert_equal assemblyFactory, Cascading::AssemblyFactory.get("assembly1")
  end


  def test_create_each  
    pipe = Cascading::AssemblyFactory.new("test").each(:filter => identity)
    assert pipe.is_a? Java::CascadingPipe::Each

    pipe = Cascading::AssemblyFactory.new("test").each("Field1", :output => "Field2", :filter => identity)

    assert pipe.is_a? Java::CascadingPipe::Each

    assert_equal "Field1", pipe.getArgumentSelector().get(0)
    assert_equal "Field2", pipe.getOutputSelector().get(0)
  end

  def test_create_every

    pipe = Cascading::AssemblyFactory.new("test").every(:aggregator => count_function)
    assert pipe.is_a? Java::CascadingPipe::Every

    pipe = Cascading::AssemblyFactory.new("test").every(:aggregator => count_function("field1", "field2"))
    assert pipe.is_a? Java::CascadingPipe::Every

    pipe = Cascading::AssemblyFactory.new("test").every("Field1", :aggregator => count_function)
    assert pipe.is_a? Java::CascadingPipe::Every
    assert_equal "Field1", pipe.getArgumentSelector().get(0)

    pipe = Cascading::AssemblyFactory.new("test").every("Field1", :aggregator => count_function, :output=>"Field2")
    assert pipe.is_a? Java::CascadingPipe::Every
    assert_equal "Field1", pipe.getArgumentSelector().get(0)
    assert_equal "Field2", pipe.getOutputSelector().get(0)
  end

  def test_create_group_by
    pipe = Cascading::AssemblyFactory.new("test").group_by("field1")

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0) 

    pipe = Cascading::AssemblyFactory.new("test").group_by("field1")

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0)
  end

  def test_create_group_by_many_fields
    pipe = Cascading::AssemblyFactory.new("test").group_by(["field1", "field2"])

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
  end

  def test_create_group_by_with_sort
    pipe = Cascading::AssemblyFactory.new("test").group_by("field1", "field2", :sort_by => ["field2"])

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    sorting_fields = pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 1, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert pipe.isSorted()
    assert !pipe.isSortReversed()
    assert_equal "field2", sorting_fields.get(0)
  end

  def test_create_group_by_with_sort_reverse
    pipe = Cascading::AssemblyFactory.new("test").group_by("field1", "field2", :sort_by => ["field2"], :reverse => true)

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    sorting_fields = pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 1, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert pipe.isSorted()
    assert pipe.isSortReversed()
    assert_equal "field2", sorting_fields.get(0)
  end

  def test_create_group_by_reverse
    pipe = Cascading::AssemblyFactory.new("test").group_by("field1", "field2", :reverse => true)

    assert pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = pipe.getGroupingSelectors()["test"]
    sorting_fields = pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 2, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert pipe.isSorted()
    assert pipe.isSortReversed()
    assert_equal "field1", sorting_fields.get(0)
    assert_equal "field2", sorting_fields.get(1)
  end
  
  
  def test_branch_empty
    assembly = Cascading::AssemblyFactory.new("test") do
      branch "branch1" do
      end
      
      branch "branch2" do
        branch "branch3" do
        end
      end
    end
    
    pipes = assembly.make
    
    assert_equal 4, pipes.size
  end
  
  def test_branch_single
    assembly = Cascading::AssemblyFactory.new("test") do      
      branch "branch1" do
        branch "branch2" do
          each "line", :function => identity
        end
      end
    end
    
    pipes = assembly.make
    
    assert_equal 3, pipes.size
  end
  
end