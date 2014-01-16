require 'dcell'
require 'celluloid/fsm'
require 'digest/md5'

module Elaine
  module Distributed
    class Worker
      include Celluloid
      include Celluloid::Logger
      include Celluloid::FSM
      


      attr_reader :vertices, :active, :vertices2
      attr_accessor :graph_size


      def initialize(coordinator_node: "elaine.coordinator", g: [], zipcodes: {}, stop_condition: Celluloid::Condition.new, partitioner: Elaine::Distributed::MD5Partitioner, out_box_buffer_size: 10_000, num_partitions: 3)
        @coordinator_node = coordinator_node
        DCell::Node[@coordinator_node][:coordinator].register_worker DCell.me.id

        @vertices = []
        @superstep_num = 0
        @stop_condition = stop_condition
        @partitioner = partitioner

        @out_box_buffer_size = out_box_buffer_size

        @outbox = []

        @num_partitions = num_partitions
        @active_count = 0
        @active = true

        # to deal with weird inheritence stuff...
        self.class.default_state :waiting
        self.class.state :waiting, to: [:running]
        self.class.state :running, to: [:finished]
        self.class.state :finished
        attach self

      end

      def active!
        debug "active! called"
        @active = true
      end

      def active?
        @active || Celluloid::Actor[:postoffice].active?
      end


      def instrumented?
        false
      end

      def set_zipcodes(zips)
        @zipcodes = zips
      end


      def add_vertices(vs)
        debug "Adding #{vs.size} vertices..."
        counter = 0
        vs.each do |v|
          counter += 1
          debug "Adding vertex ##{counter}" if counter % 1_000 == 0
          add_vertex v
        end
      end

      def add_vertex(v)
        @vertices2 ||= []

        vertex = v[:klazz].new v[:id], v[:value], Celluloid::Actor[:postoffice], v[:outedges]
        @vertices2 << vertex

        @active_count += 1

      end

      def init_graph(g=[])
        raise 'empty worker graph' if g.empty?
        
        @vertices = []

        # HACK the local vertices should be dealt with differently than
        # the @vertices2 member
        @vertices2 = []
        g.each do |n|
          @vertices << n[:id]
          v = n[:klazz].new n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          @vertices2 << v
        end
        @active_count = @vertices.size

        debug "There are #{@vertices.size} vertices in this worker."

      end

      # HACK this should be handled better...
      def init_superstep
        
        transition :running
        
        debug "Starting init_superstep"
        @active = false
        counter = 0
        @vertices2.each_slice(5_000) do |slice|

          counter += 1
          debug "Loading messages for slice ##{counter}" if counter % 10 == 0
          
          slice_ids = []
          slice.each do |v|
            slice_ids << v.id
          end
          
          boxes = Celluloid::Actor[:postoffice].bulk_read_all(slice_ids)
          slice.each do |v|
            v.messages = boxes[v.id]
            v.active! if v.messages.size > 0
            active! if v.active?
          end
        end
        Celluloid::Actor[:postoffice].init_superstep
        debug "#{DCell.me.id} finished init_superstep"
      end

      def pmap(enum, &block)
        futures = enum.map { |elem| Celluloid::Future.new(elem, &block) }
        futures.map { |future| future.value }
      end

      def stop
        @stop_condition.signal(true)
      end

      def superstep
        @active = false
        # might improve performance not to bother with this active check
        # here and just loop through all vertices and execute if they are active
        # not sure how it will interact with threads, could end up being a
        # thread that has no active vertices...
        # active = @vertices2.select {|v| v.active?}
        active = @vertices2
        debug "There are #{active.size} active vertices in this step"

        # we are going to make 4 slices and run them each in a thread
        debug "active.size: #{active.size}"
        slice_size = (active.size / @num_partitions).to_i
        slice_size = 1 if slice_size < 1
        debug "slice size: #{slice_size}"
        slices = active.each_slice(slice_size)

        pmap(slices) do |s|
          s.each do |v|
            if v.active?
              v.step
              active! if v.active?
            end
          end
        end
      
        info "Delivering all messages from end of super step."
        Celluloid::Actor[:postoffice].deliver_all
        info "Finished super step"

        @active_count = active.select {|v| v.active?}.size    
      end

      def vertex_values
        @vertices2.map { |v| {id: v.id, value: v.value} }
      end

      def finish!
        transition :finished
      end

    end # class Worker
  end # module Distributed
end # module Elaine
