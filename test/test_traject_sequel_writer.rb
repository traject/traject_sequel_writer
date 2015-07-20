require 'test_helper'

require 'traject/sequel_writer'

describe "Traject::SequelWriter" do
  describe "in use" do
    before do
      @writer = writer
      write_mock_docs(@writer, 63)      
    end

    after do
      @writer.db_table.delete
    end

    it "writes" do
      assert_equal 63, @writer.db_table.count

      @writer.db_table.each do |hash|
        assert_kind_of String, hash[:string_a], "string_a is not filled out for #{hash}"
        assert_kind_of String, hash[:string_b], "string_b is not filled out for #{hash}"
        assert_nil hash[:string_c]
        assert_includes [true, false], hash[:boolean_a], "boolean_a is not true or false for #{hash}"
        assert_kind_of Integer, hash[:int_a]
      end
    end
  end

  describe "with multiple values" do
    it "joins multiple string values" do
      @writer = self.writer

      context = Traject::Indexer::Context.new
      context.output_hash.merge!(
        "string_a" => ["String_One", "String_Two"],
        "string_b" => ["String_B_One"]
      )
      @writer.put context  
      @writer.close

      assert @writer.db_table.where(:string_a => "String_One,String_Two", :string_b => "String_B_One").count == 1, "Expected written row with expected values"
    end

    after do
      @writer.db_table.delete
    end
  end


  it "writes with sequel.database parameter instead of connection_str" do
    sequel_db = Sequel.connect(TEST_SEQUEL_CONNECT_STR)

    writer = writer("sequel_writer.connection_string" => nil, 
      "sequel_writer.database" => sequel_db)

    write_mock_docs(writer, 63)

    assert_equal 63, writer.db_table.count

    writer.db_table.each do |hash|
      assert_kind_of String, hash[:string_a], "string_a is not filled out for #{hash}"
      assert_kind_of String, hash[:string_b], "string_b is not filled out for #{hash}"
      assert_nil hash[:string_c]
      assert_includes [true, false], hash[:boolean_a], "boolean_a is not true or false for #{hash}"
      assert_kind_of Integer, hash[:int_a]
    end

    writer.db_table.delete
  end

  describe "after_send_batch" do
    before do
      @writer = writer

      @received_in_batches = Queue.new

      @writer.after_send_batch do |batch, writer|
        assert_kind_of Traject::SequelWriter, writer
        assert_kind_of Array, batch       

        batch.each {|c| @received_in_batches.push c }
      end

      @num_docs = 63

      write_mock_docs(@writer, @num_docs)
    end

    after do      
      @writer.db_table.delete
    end

    it "calls callbacks" do
      assert_equal @num_docs, @received_in_batches.size
    end
  end

  describe "errors" do
    it "raises without required settings" do
      assert_raises(ArgumentError) { Traject::SequelWriter.new }
      assert_raises(ArgumentError) { Traject::SequelWriter.new("sequel_writer.connect_string" => "foo") }
      assert_raises(ArgumentError) { Traject::SequelWriter.new("sequel_writer.table_name" => "foo") }
    end
  end


  # Helpers

  def write_mock_docs(writer, num)
    (1..num).each do |i|
        context = Traject::Indexer::Context.new
        context.output_hash.merge!(
          "id" => ["ignore_me"], # should ignore pk by default
          "string_a" => ["String_a #{i}"],
          "string_b" => ["String_b #{i}"],
          "no_such_column" => ["ignore me"],
          "boolean_a" => [(i % 2 == 0) ? true : false],
          "int_a" => [i]
        )
        writer.put context
      end
      writer.close
  end

  def writer(args = {})
    args = {"sequel_writer.connection_string" => TEST_SEQUEL_CONNECT_STR,
        "sequel_writer.table_name" => "test",
        "sequel_writer.batch_size" => 5}.merge(args)
    return Traject::SequelWriter.new(args)
  end


end
