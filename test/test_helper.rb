require 'minitest/autorun'
require 'minitest/spec'

require 'traject_sequel_writer'

require 'minitest/autorun'

require 'sequel'

# Create a temporary sqlite db for tests, remove it when we're done
require 'fileutils'
FileUtils::mkdir_p 'tmp'


TEST_SEQUEL_CONNECT_STR = if defined? JRuby
  "jdbc:sqlite:tmp/testing.sqlite"
else
  "sqlite://tmp/testing.sqlite"
end

db = Sequel.connect(TEST_SEQUEL_CONNECT_STR)
db.drop_table?(:test)
db.create_table(:test) do
  primary_key :id
  Time :created_at
  String :string_a
  String :string_b
  String :string_c
  TrueClass :boolean_a
  Integer   :int_a
end

db[:test].delete
# Disconnect it now, make the code do the work
db.disconnect



MiniTest.after_run do 
  File.unlink("tmp/testing.sqlite")
end