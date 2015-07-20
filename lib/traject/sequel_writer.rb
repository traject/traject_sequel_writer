require 'traject'
require 'traject/util'
require 'traject/indexer/settings'

require 'sequel'

require 'thread'

module Traject
  class SequelWriter
    # Sequel db connection object
    attr_reader :sequel_db

    # Sequel table/relation object
    attr_reader :db_table

    def initialize(argSettings)
      @settings = Traject::Indexer::Settings.new(argSettings)

      unless (!! @settings["sequel_writer.connection_string"]) ^ (!! @settings["sequel_writer.database"]) 
        raise ArgumentError, "Exactly one of either setting `sequel_writer.connection_string` or `sequel_writer.database` is required"
      end
      unless @settings["sequel_writer.table_name"]
        raise ArgumentError, "setting `sequel_writer.table_name` is required"
      end

      @disconnect_on_close = true
      @sequel_db = @settings["sequel_writer.database"]
      unless @sequel_db
        @sequel_db = Sequel.connect(@settings["sequel_writer.connection_string"])
        @disconnect_on_close = false
      end

      @db_table  = @sequel_db[  @settings["sequel_writer.table_name"].to_sym ]


      # Which keys to send to columns? Can be set explicitly with sequel_writer.columns,
      # or we'll use all non-PK columns introspected from the db schema. 
      @column_names      = @settings["sequel_writer.columns"]

      unless @column_names
        @column_names = @sequel_db.schema( @db_table.first_source_table ).find_all do |column, info|
          info[:primary_key] != true
        end.collect {|pair| pair.first}
      end
      @column_names = @column_names.collect {|c| c.to_sym}
      @column_names = @column_names.freeze

      
      # How many threads to use for the writer?
      # if our thread pool settings are 0, it'll just create a null threadpool that
      # executes in calling context. Default to 1, for waiting on DB I/O. 
      @thread_pool_size = (@settings["sequel_writer.thread_pool"] || 1).to_i

      @batch_size       = (@settings["sequel_writer.batch_size"] || 100).to_i

      @batched_queue         = Queue.new
      @thread_pool = Traject::ThreadPool.new(@thread_pool_size)

      @after_send_batch_callbacks = Array(@settings["sequel_writer.after_send_batch"] || [])

      @internal_delimiter = @settings["sequel_writer.internal_delimiter"] || ","
    end

    # Get the logger from the settings, or default to an effectively null logger
    def logger
      @settings["logger"] ||= Yell.new(STDERR, :level => "gt.fatal") # null logger
    end

    def put(context)
      @thread_pool.raise_collected_exception!

      @batched_queue << context
      if @batched_queue.size >= @batch_size
        batch = Traject::Util.drain_queue(@batched_queue)
        @thread_pool.maybe_in_thread_pool(batch) {|batch_arg| send_batch(batch_arg) }
      end
    end

    def close
      @thread_pool.raise_collected_exception!

      # Finish off whatever's left. Do it in the thread pool for
      # consistency, and to ensure expected order of operations, so
      # it goes to the end of the queue behind any other work.
      batch = Traject::Util.drain_queue(@batched_queue)
      @thread_pool.maybe_in_thread_pool(batch) {|batch_arg| send_batch(batch_arg) }
      

      # Wait for shutdown, and time it.
      logger.debug "#{self.class.name}: Shutting down thread pool, waiting if needed..."
      elapsed = @thread_pool.shutdown_and_wait
      if elapsed > 60
        logger.warn "Waited #{elapsed} seconds for all threads, you may want to increase sequel_writer.thread_pool (currently #{@settings["solr_writer.thread_pool"]})"
      end
      logger.debug "#{self.class.name}: Thread pool shutdown complete"

      # check again now that we've waited, there could still be some
      # that didn't show up before.
      @thread_pool.raise_collected_exception!

      @sequel_db.disconnect if @disconnect_on_close
    end

    def send_batch(batch)
      list_of_arrays = hashes_to_arrays(@column_names, batch.collect {|context| context.output_hash})

      begin
        db_table.import @column_names, list_of_arrays
      rescue Sequel::DatabaseError, Sequel::PoolTimeout => batch_exception
        # We rescue PoolTimeout too, because we're mysteriously getting those, they are maybe dropped DB connections?
        # Try them each one by one, mostly so we can get a reasonable error message with particular record. 
        logger.warn("SequelWriter: error (#{batch_exception}) inserting batch of #{list_of_arrays.count} starting from system_id #{batch.first.output_hash['system_id']}, retrying individually...")
        
        batch.each do |context|
          send_single(context)
        end
      end

      @after_send_batch_callbacks.each do |callback|
        callback.call(batch, self)
      end
    end

    def send_single(context)      
      db_table.insert @column_names, hash_to_array(@column_names, context.output_hash)
    rescue Sequel::DatabaseError => e
      logger.error("SequelWriter: Could not insert row: #{context.output_hash}: #{e}")
      raise e
    end


    # Turn an array of hashes into an array of arrays,
    # with each array being a hashes values matching column_names, in that order
    def hashes_to_arrays(column_names, list_of_hashes)
      list_of_hashes.collect do |h| 
        hash_to_array(column_names, h)
      end
    end

    def hash_to_array(column_names, hash)
      column_names.collect do |c| 
        output_value_to_column_value(hash[c.to_s])
      end
    end

    # Traject context.output_hash values are arrays.
    # turn them into good column values, joining strings if needed. 
    #
    # Single values also accepted, even though not traject standard, they
    # will be passed through unchanged. 
    def output_value_to_column_value(v)
      if v.kind_of?(Array)
        if v.length == 0
          nil
        elsif v.length == 1
          v.first
        elsif v.first.kind_of?(String)
          v.join(@internal_delimiter)
        else
          # Not a string? Um, raise for now?
          raise ArgumentError.new("Traject::SequelWriter, multiple non-String values provided: #{v}")
        end
      else
        v
      end
    end


    def after_send_batch(&block)
      @after_send_batch_callbacks << block
    end

  end
end