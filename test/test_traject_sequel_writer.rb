require 'test_helper'

require 'traject/sequel_writer'

describe "Traject::SequelWriter" do
  describe "in use" do
    before do
      @writer = Traject::SequelWriter.new(
        "sequel_writer.connection_string" => "sqlite://tmp/testing.sqlite",
        "sequel_writer.table_name" => "test",
        "sequel_writer.batch_size" => 5
      )

      (1..63).each do |i|
        context = Traject::Indexer::Context.new
        context.output_hash.merge!(
          "string_a" => "String_a #{i}",
          "string_b" => "String_b #{i}",
          "no_such_column" => "ignore me",
          "boolean_a" => (i % 2 == 0) ? true : false,
          "int_a" => i
        )

        @writer.put context

      end
      @writer.close
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

    it "sets created_at" do
      skip

      @writer.db_table.each do |hash|
        assert_kind_of DateTime, hash[:created_at]
      end
    end

    
  end
end
