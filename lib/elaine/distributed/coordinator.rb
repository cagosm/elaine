# require 'dnssd'
# require 'celluloid/io'
require 'dcell'
# require 'elaine/instruments'

module Elaine
  module Distributed
    class Coordinator
      include Celluloid
      include Celluloid::FSM
      include Celluloid::Logger
      # include Elaine::Instruments::Instrument
      # finalizer :shutdown

      attr_reader :workers
      attr_reader :partitions
      attr_reader :num_partitions
      

      def initialize(graph: nil, num_partitions: 1, stop_condition: Celluloid::Condition.new, finished_condition: Celluloid::Condition.new, partitioner: Elaine::Distributed::MD5Partitioner, graph_size: 0)
        @workers = []
        @num_partitions = num_partitions
        @graph = graph
        info "GOT GRAPH: #{graph}"
        @partitions = Hash.new
        @stop_condition = stop_condition
        @partitioner = partitioner
        @zipcodes = zipcodes
        @finished_condition = finished_condition
        @graph_size = graph_size
        # @instrument = instrument

        # to deal with weird inheritence stuff...
        self.class.default_state :waiting
        self.class.state :waiting, to: [:running]
        self.class.state :running, to: [:finished]
        self.class.state :finished
        attach self
      end



      def graph=(g)
        debug "Setting graph"
        @graph = g
        debug "done setting graph"
      end

      def zipcodes
        zips = {}
        @partitions.each_pair do |zip, range|
          # vertices.each do |vertex|
            # zips[vertex[:id]] = zip
          # end
          zips[range] = zip
        end
        zips
      end

      def distribute_graph(zips)
        # debug "distributing graph"
        # let's try to split the graph up into 1k vertex slices and then add
        # them to workers in parallel... might speed things up
        remainder = @graph

        zips.each_pair do |range, worker_node|
          # to_add = @graph.select { |v| range.include? @partitioner.key(v[:id]) }
          to_add, remainder = remainder.partition { |v| range.include? @partitioner.key(v[:id])}
          debug "there are #{to_add.size} vertices to add to #{zips[range]} [#{range.first}, #{range.last}]"
          to_add.each_slice(1_000) do |slice|
            debug "adding slice"
            DCell::Node[worker_node][:worker].add_vertices slice
            debug "finished adding slice"
            
          end
          debug "finished adding all #{to_add.size} vertices to #{zips[range]}"
          
        end

        raise "Still #{remainder.size} vertices left to distribute!" if remainder.size > 0

        @graph.size
      end

      def pmap(enum, &block)
        futures = enum.map { |elem| Celluloid::Future.new(elem, &block) }
        futures.map { |future| future.value }
      end

      def init_job
        
        debug "partitioning"
        partition
        
        # distribute the zipcodes
        debug "building zipcodes"
        zips = zipcodes
        debug "distributing zipcodes"
        @workers.each do |worker_node|
          debug "Sending zipcodes to: #{worker_node}"
          # DCell::Node[worker_node][:worker].zipcodes = zips
          DCell::Node[worker_node][:postoffice].zipcodes = zips
        end


        # now send the graph
        debug "distributing graph"
        distribute_graph zips
        debug "done distributing graph"
      end

      def partition
       
        debug "running partitioner: #{@partitioner}"
        tmp_partitions = @partitioner.partition(@graph.map {|v| v[:id]}, @workers.size)
        debug "done partitioner: #{@partitioner}"

        debug "tmp_partititions: #{tmp_partitions}"
        
        # now we need to map the partitions back to the workers.
        tmp_partitions.each_with_index do |p, idx|
          @partitions[@workers[idx]] = p
        end
        debug "clearing tmp_partitions..."
        tmp_partitions.clear
        debug "done clearing tmp_partitions..."

        debug "@partitions: #{@partitions}"

      end

      def register_worker(worker_node)
        # we could, in theory, have multiple workers in the same node, however
        # i think it makes more sense to just have multiple nodes running on the
        # same machine instead of multiple workers in a single node
        # This should be re-evaluated at some point in the future.
        info "Registering worker: #{worker_node}"
        unless @workers.include? worker_node
          @workers << worker_node
        end
      end

      def init_superstep
         # execute a superstep and wait for workers to complete
          debug "Initializing superstep"

          step = @workers.map { |w| DCell::Node[w][:worker].future(:init_superstep)}
          step.map { |f| f.value }

      end

      def superstep
        init_superstep
      
        debug "Executing superstep on workers"
        step = @workers.map { |w| DCell::Node[w][:worker].future(:superstep)}
        step.map { |f| f.value }
        debug "Finished executing superstep on workers"
      end

      def active_worker_count
        to_test = @workers.map { |w| DCell::Node[w][:worker].future(:active?)}
        to_test.map { |f| f.value ? 1 : 0 }.reduce(:+)
      end

      def run_until_finished
        
        init_job

        debug "Running job"
        step_num = 0
        loop do
          step_num += 1
          debug "Running superstep #{step_num}"
          superstep
      
          break if active_worker_count.zero?
        end
        debug "Job finished!"
      end

      def start_job
        transition :running
        run_job
        transition :finished
      end

      def run_job
        run_until_finished
      end

      def finish!
        transition :finished
        @workers.each do |w|
          DCell::Node[w][:worker].finish!
        end
        @finished_condition.signal(true)
      end

      def stop
        @workers.each do |w|
          DCell::Node[w][:worker].async.stop
        end
        @stop_condition.signal(true)
      end

      def run_and_stop
        run_until_finished
        @stop_condition.signal(true)
      end

      def vertex_values(&block)
        @workers.map do |w|
          worker_node = DCell::Node[w]
          worker_node[:worker].vertex_values
        end.flatten
      end

      def instrumented?
        false
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
