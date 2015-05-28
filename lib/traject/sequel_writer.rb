require 'traject'
require 'traject/util'
require 'traject/indexer/settings'

require 'sequel'

module Traject
  class SequelWriter
    # Sequel db connection object
    attr_reader :sequel_db

    # Sequel table/relation object
    attr_reader :db_table

    def initialize(argSettings)
      @settings = Traject::Indexer::Settings.new(argSettings)


      @sequel_db = Sequel.connect(@settings["sequel_writer.connection_string"])
      @db_table  = @sequel_db[  @settings["sequel_writer.table_name"].to_sym ]

      @pk_column = (@settings["sequel_writer.pk_column"] || (
        # use Sequel schema lookup
        (@sequel_db.schema( @db_table.first_source_table ).find {|column, info| info[:primary_key] == true}).first
      )).to_sym

      @column_names      = (@settings["sequel_writer.columns"] || (@db_table.columns - [@pk_column])).collect {|c| c.to_sym}

      # How many threads to use for the writer?
      # if our thread pool settings are 0, it'll just create a null threadpool that
      # executes in calling context. Default to 1, for waiting on DB I/O. 
      @thread_pool_size = (@settings["sequel_writer.thread_pool"] || 1).to_i

      @batch_size       = (@settings["sequel_writer.batch_size"] || 100).to_i

      @batched_queue         = Queue.new
      @thread_pool = Traject::ThreadPool.new(@thread_pool_size)

      @after_send_batch_callbacks = Array(@settings["sequel_writer.after_send_batch"] || [])
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
      @thread_pool.maybe_in_thread_pool { send_batch(batch) }
      

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

      @sequel_db.disconnect
    end

    def send_batch(batch)
      list_of_arrays = hashes_to_arrays(@column_names, batch.collect {|context| context.output_hash})

      db_table.import @column_names, list_of_arrays

      @after_send_batch_callbacks.each do |callback|
        callback.call(batch, self)
      end
    end

    # Turn an array of hashes into an array of arrays,
    # with each array being a hashes values matching column_names, in that order
    def hashes_to_arrays(column_names, list_of_hashes)
      list_of_hashes.collect do |h| 
        column_names.collect {|c| h[c.to_s] }
      end
    end

    def after_send_batch(&block)
      @after_send_batch_callbacks << block
    end

  end
end