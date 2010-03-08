#! /usr/bin/ruby

require 'test/unit'
require 'cascading/assembly'

def compare_with_references(test_name)
  result = compare_files("test/references/#{test_name}.txt", "output/#{test_name}/part-00000")
  assert_nil(result)
end

class TC_Assembly < Test::Unit::TestCase

  include Cascading::Operations

  def test_create_assembly_simple
    assembly = Cascading::Assembly.new("assembly1") do
    end

    assert_not_nil Cascading::Assembly.get("assembly1")
    assert_equal assembly, Cascading::Assembly.get("assembly1")
    pipe = assembly.tail_pipe
    assert pipe.is_a? Java::CascadingPipe::Pipe
  end

  def test_each_identity
    assembly = Cascading::Assembly.new("assembly1") do
      each "field1", :filter => identity
    end

    assert_not_nil Cascading::Assembly.get("assembly1")
    assert_equal assembly, Cascading::Assembly.get("assembly1")
  end


  def test_create_each  
    assembly = Cascading::Assembly.new("test") do
      each(:filter => identity)
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Each

    assembly = Cascading::Assembly.new("test") do 
      each("Field1", :output => "Field2", :filter => identity)
    end
    pipe = assembly.tail_pipe


    assert pipe.is_a? Java::CascadingPipe::Each

    assert_equal "Field1", pipe.getArgumentSelector().get(0)
    assert_equal "Field2", pipe.getOutputSelector().get(0)
  end

  def test_create_every

    assembly = Cascading::Assembly.new("test") do
      every(:aggregator => count_function)
    end
    pipe = assembly.tail_pipe
    assert pipe.is_a? Java::CascadingPipe::Every

    assembly = Cascading::Assembly.new("test") do 
      every(:aggregator => count_function("field1", "field2"))
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every

    assembly = Cascading::Assembly.new("test") do
      every("Field1", :aggregator => count_function)
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
    assert_equal "Field1", assembly.tail_pipe.getArgumentSelector().get(0)

    assembly = Cascading::Assembly.new("test") do
      every("Field1", :aggregator => count_function, :output=>"Field2")
    end
    assert assembly.tail_pipe.is_a? Java::CascadingPipe::Every
    assert_equal "Field1", assembly.tail_pipe.getArgumentSelector().get(0)
    assert_equal "Field2", assembly.tail_pipe.getOutputSelector().get(0)
  end

  def test_create_group_by
    assembly = Cascading::Assembly.new("test") do
      group_by("field1")
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0) 

    assembly = Cascading::Assembly.new("test") do
      group_by("field1")
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0)
  end

  def test_create_group_by_many_fields
    assembly = Cascading::Assembly.new("test") do
      group_by(["field1", "field2"])
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
  end

  def test_create_group_by_with_sort
    assembly = Cascading::Assembly.new("test") do
      group_by("field1", "field2", :sort_by => ["field2"])
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    sorting_fields = assembly.tail_pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 1, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert assembly.tail_pipe.isSorted()
    assert !assembly.tail_pipe.isSortReversed()
    assert_equal "field2", sorting_fields.get(0)
  end

  def test_create_group_by_with_sort_reverse
    assembly = Cascading::Assembly.new("test") do
      group_by("field1", "field2", :sort_by => ["field2"], :reverse => true)
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    sorting_fields = assembly.tail_pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 1, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert assembly.tail_pipe.isSorted()
    assert assembly.tail_pipe.isSortReversed()
    assert_equal "field2", sorting_fields.get(0)
  end

  def test_create_group_by_reverse
    assembly = Cascading::Assembly.new("test") do 
      group_by("field1", "field2", :reverse => true)
    end

    assert assembly.tail_pipe.is_a? Java::CascadingPipe::GroupBy
    grouping_fields = assembly.tail_pipe.getGroupingSelectors()["test"]
    sorting_fields = assembly.tail_pipe.getSortingSelectors()["test"]

    assert_equal 2, grouping_fields.size
    assert_equal 2, sorting_fields.size

    assert_equal "field1", grouping_fields.get(0)
    assert_equal "field2", grouping_fields.get(1)
    assert assembly.tail_pipe.isSorted()
    assert assembly.tail_pipe.isSortReversed()
    assert_equal "field1", sorting_fields.get(0)
    assert_equal "field2", sorting_fields.get(1)
  end

  def test_branch_unique
    assembly = Assembly.new("test") do
      branch "branch1"
    end

    assert_equal 1, assembly.children.size

  end

  def test_branch_empty
    assembly = Cascading::Assembly.new("test") do
      branch "branch1" do
      end

      branch "branch2" do
        branch "branch3" 
      end
    end

    assert_equal 2, assembly.children.size
    assert_equal 1, assembly.children[1].children.size

  end

  def test_branch_single
    assembly = Cascading::Assembly.new("test") do      
      branch "branch1" do
        branch "branch2" do
          each "line", :function => identity
        end
      end
    end

    assert_equal 1, assembly.children.size
    assert_equal 1, assembly.children[0].children.size

  end

  def test_full_assembly
    assembly = Cascading::Assembly.new "test" do
      each("Field1", :output => "Field1", :filter => identity)
      every(:aggregator => count_function)
    end


    pipe = assembly.tail_pipe

    assert pipe.is_a? Java::CascadingPipe::Every

  end

end


class TC_AssemblyScenarii < Test::Unit::TestCase

  def test_splitter
    flow = Cascading::Flow.new("splitter") do

      source "copy", tap("test/data/data1.txt")
      sink "copy", tap('output/splitter', :sink_mode => :replace)

      assembly "copy" do

        split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]

        assert_size_equals 4

        assert_not_null

        debug :print_fields=>true
      end
    end
    # Had to wrap this in a CascadingException so that I could see the message
    # of the deepest cause -- which told me the output already existed.
    #
    # We can safely wrap all calls to Cascading in CE once we change it to
    # print the stack trace of -every- exception in the cause chain (otherwise
    # it eats the stack trace and you can't dig down into the Cascading code).
    begin
      flow.complete
    rescue NativeException => e
      throw CascadingException.new(e, 'Flow failed to complete')
    end
  end


  def test_join1
    flow = Cascading::Flow.new("splitter") do

      source "data1", tap("test/data/data1.txt")
      source "data2", tap("test/data/data2.txt")
      sink "joined", tap('output/joined', :sink_mode => :replace)

      assembly1 = assembly "data1" do

        split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]
        
        assert_size_equals 4

        assert_not_null
        debug :print_fields=>true

      end

      assembly2 = assembly "data2" do

        split "line", :pattern => /[.,]*\s+/, :into=>["name",  "id", "town"], :output => ["name",  "id", "town"]

        assert_size_equals 3

        assert_not_null
        debug :print_fields=>true
      end

      assembly "joined" do
        join assembly1, assembly2, :on => ["name", "id"], :declared_fields => ["name", "score1", "score2", "id", "name2", "id2", "town"]
      
        assert_size_equals 7

        assert_not_null
        
      end
    end
    flow.complete
  end
  
  def test_join2
     flow = Cascading::Flow.new("splitter") do

       source "data1", tap("test/data/data1.txt")
       source "data2", tap("test/data/data2.txt")
       sink "joined", tap('output/joined', :replace=>true)

       assembly1 = assembly "data1" do

         split "line", :pattern => /[.,]*\s+/, :into=>["name", "score1", "score2", "id"], :output => ["name", "score1", "score2", "id"]

         debug :print_fields=>true

       end

       assembly2 = assembly "data2" do

         split "line", :pattern => /[.,]*\s+/, :into=>["name",  "code", "town"], :output => ["name",  "code", "town"]

         debug :print_fields=>true
       end

       assembly "joined" do
         join :on => {assembly1=>["name", "id"], assembly2=>["name", "code"]}, :declared_fields => ["name", "score1", "score2", "id", "name2", "code", "town"]
       end
     end
    flow.complete
   end
end
