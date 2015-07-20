[![Build Status](https://travis-ci.org/traject/traject_sequel_writer.svg)](https://travis-ci.org/traject/traject_sequel_writer)
[![Gem Version](https://badge.fury.io/rb/traject_sequel_writer.svg)](http://badge.fury.io/rb/traject_sequel_writer)

# TrajectSequelWriter

A `Writer` plugin for [traject](https://github.com/traject/traject), that writes to an rdbms using [Sequel](https://github.com/jeremyevans/sequel).

The writer can be used as standard, as the destination of your indexing pipeline. 

It was actually written for a use case where it's used as a "side effect" in a traject `each_record`, writing different data out to an rdbms on the side, while the main indexing is to Solr. This ends up a bit hacky at present but works.  


## Installation

We recommend using bundler with a traject project that has dependencies.
Add this line to your traject project's Gemfile:

```ruby
gem 'traject_sequel_writer', "~> 1.0"
```

And then execute:

    $ bundle

Or if you don't use bundler with your traject project, install directly like: 

    $ gem install traject_sequel_writer

## Usage

As a standard traject writer, you can just set a few settings:

~~~ruby
settings do
  provide "writer_class_name", "Traject::SequelWriter"
  # ...
~~~

You can set up the connection with a [Sequel connection string](http://sequel.jeremyevans.net/rdoc/files/doc/opening_databases_rdoc.html). Here's
an example for JDBC mysql (under JRuby). Note we discovered the `&characterEncoding=utf8` arg to the JDBC adapter was important. 
Connection string parameters may vary for platform (MRI vs JDBC in Jruby) and database. 

~~~ruby
  provide "sequel_writer.connection_string", "jdbc:mysql://dbhost.example.com:3306/database_name?characterEncoding=utf8&user=user&password=password"
  # You also need to tell the writer what table to write to; the table should already exist
  provide "sequel_writer.table_name", "my_table"
~~~

By default, the writer will try to write to every non-pk column defined in your table -- if your Traject::Context output_hash's is missing
a value for a column, null will be inserted for that column. Or you can explicitly define which columns to use:

~~~ruby
  provide "sequel_writer.columns", ["column1", "column2"]
~~~~

Still, your Context output_hash's must provide output key/values for every column mentioned, or else
null will be inserted for that column. Keys in the output_hash that don't match output columns
will be ignored. 

Note that traject output_hash's have values that are arrays of potentially multiple values. If
multiple values are present, they will be joined with a comma or with set `sequel_writer.internal_delimiter`. 
For non-string type db fields, this will probably raise. 
`traject_sequel_writer` also accepts single values in output_hash as an alternative, which isn't really traject's
API, but experimenting to see if it's helpful rather than confusing to accept this alternate too. 
 
### All settings

* `sequel_writer.connection_string` : [Sequel connection string](http://sequel.jeremyevans.net/rdoc/files/doc/opening_databases_rdoc.html)
* `sequel_writer.database`: As an alternative to `sequel_connection_string`, pass in an already instantiated Sequel::Database object, as in from `Sequel.connect`
* `sequel_writer.table_name`: Required, what table to write to. 
* `sequel_writer.column_names` Which columns to write to, by default all non-pk columns in the table. Since we use multi-row import statements,
  column_names not present in the Traject::Context#output_hash will end up with SQL `null` inserted. 
* `sequel_writer.thread_pool_size` Number of threads to use for writing to DB. Default 1, should be good. 
* `sequel_writer.batch_size` Count of records to batch together in a single multi-row SQL `INSERT`. Default 100. Should be good. 
* `sequel_writer.internal_delimiter` -- Delimiter _within_ a field, for multiple values. Default is comma.

### Using as a side-channel additional output 

In one project, we wanted to index to Solr. But we wanted to calculate completely different output to send
as a side-channel to an RDBMS table. Here's a little bit hacky way to do that, that would really work
for any traject writer. 

~~~ruby
sequel_writer = Traject::SequelWriter.new(
  'sequel_writer.connection_string' => conn_str, 
  'sequel_writer.table_name' => 'my_table')

each_record do |record, context|
  # imagine this returns an array of hashes, each of which represents a row
  # you want to insert into the table. 

  rows_to_insert = make_rows_to_insert(record)

  rows_to_insert.each do |row|
     # Don't re-use the variable name `context`, can cause accidental shared concurrent state
     sequel_writer.put( Traject::Context.new(:output_hash => row, :source_record => record) )
   end
end

# Don't forget to close our side-channel sequel writer, to make
# sure anything queued gets written
after_processing do
  sequel_writer.close
end
~~~



## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release` to create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

1. Fork it ( https://github.com/[my-github-username]/traject_sequel_writer/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
