require 'dcell'

module Elaine
  module Distributed
    class Worker
      include Celluloid
      include Celluloid::Logger

      attr_reader :vertices, :active, :vertices2


      def initialize(coordinator_node: "elaine.coordinator", g: [], zipcodes: {}, stop_condition: Celluloid::Condition.new)

        # @coordinator_node = DCell::Node["elaine.coordinator"]
        @coordinator_node = coordinator_node
        DCell::Node[@coordinator_node][:coordinator].register_worker DCell.me.id

        @vertices = []
        @superstep_num = 0
        @stop_condition = stop_condition
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
        @active = @vertices2.select { |v| v.active? }.size

      end

      def init_graph(g=[])
        raise 'empty worker graph' if g.empty?
        if @vertices.size > 0
          @vertices.each do |v|
            # Celluloid::Actor[v].terminate
          end
        end
        @vertices = []
        # raise "Graph already initialized!" if @vertices.size > 0

        # we are going to assume that graphs come in as json documents
        # *describing* the graph.
        # @vertices = graph
        # @active   = graph.size

        # HACK the local vertices should be dealt with differently than
        # the @vertices2 member
        @vertices2 = []
        g.each do |n|
          # n[:klazz].supervise_as n[:id], n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          @vertices << n[:id]
          v = n[:klazz].new n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          @vertices2 << v
        end
        @active = @vertices.size

        debug "There are #{@vertices.size} vertices in this worker."

      end

      # HACK this should be handled better...
      def init_superstep
        debug "Starting init_superstep"
        counter = 0
        @vertices2.each_slice(5_000) do |slice|

          counter += 1
          debug "Loading messages for slice ##{counter}" if counter % 10 == 0
          # v.messages = Celluloid::Actor[:postoffice].read_all(v.id)
          slice_ids = []
          slice.each do |v|
            slice_ids << v.id
          end
          boxes = Celluloid::Actor[:postoffice].bulk_read_all(slice_ids)
          slice.each do |v|
            v.messages = boxes[v.id]
          end
        end
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
        active = @vertices2.select {|v| v.active?}
        debug "There are #{active.size} active vertices in this step"

        # we are going to make 4 slices and run them each in a thread
        debug "active.size: #{active.size}"
        slice_size = (active.size / 2).to_i
        slice_size = 1 if slice_size < 1
        debug "slice size: #{slice_size}"
        slices = active.each_slice(slice_size)

        pmap(slices) do |s|
          s.each do |v|
            v.step
          end
        end
        
        # pmap(active) do |v|
        #   v.step
        # end
        
        # active.each do |v|
        #   v.step
        # end

        debug "Delivering all messages from end of super step."
        Celluloid::Actor[:postoffice].deliver_all

        debug "Finished super step"

        @active = active.select {|v| v.active?}.size    
      end

      def vertex_values
        @vertices2.map { |v| {id: v.id, value: v.value} }
      end
    end # class Worker
  end # module Distributed
end # module Elaine
