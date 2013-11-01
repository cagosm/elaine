require 'dcell'

module Elaine
  module Distributed
    class Worker
      include Celluloid
      include Celluloid::Logger

      attr_reader :vertices, :active, :vertices2

      # def initialize(graph = [], id: "elaine-worker", addr: "tcp://127.0.0.1:8096")
      #   raise 'empty worker graph' if graph.empty?
      #   @vertices = graph
      #   @active   = graph.size
      # end

      def initialize(coordinator_node: "elaine.coordinator", g: [], zipcodes: {})

        # @coordinator_node = DCell::Node["elaine.coordinator"]
        @coordinator_node = coordinator_node
        DCell::Node[@coordinator_node][:coordinator].register_worker DCell.me.id

        # Elaine::Distributed::PostOffice.supervise_as :postoffice
        # this could/should probably be done in the constructor
        # Celluloid::Actor[:postoffice].zipcodes = zipcodes
        @vertices = []
        @superstep_num = 0
        # self.init_graph(g)
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
        # need to delivery all messages here first, to avoid race conditions
        # @vertices.each do |v|
        #   vertex = Celluloid::Actor[v]
        #   # v.messages = PostOffice.instance.read(v.id)
        #   vertex.messages = Celluloid::Actor[:postoffice].read_all(vertex.id)
        #   vertex.active! if vertex.messages.size > 0
        # end

        @vertices2.each do |v|
          v.messages = Celluloid::Actor[:postoffice].read_all(v.id)
        end
        debug "#{DCell.me.id} finished init_superstep"
      end

      def pmap(enum, &block)
        futures = enum.map { |elem| Celluloid::Future.new(elem, &block) }
        futures.map { |future| future.value }
      end

      def superstep
        # Thread.new do
        # @superstep_num += 1
        # debug "Running superstep #{@superstep_num}"
        # @vertices.each do |v|
        #   vertex = Celluloid::Actor[v]
        #   # v.messages = PostOffice.instance.read(v.id)
        #   vertex.messages = Celluloid::Actor[:postoffice].read_all(vertex.id)
        #   vertex.active! if vertex.messages.size > 0
        # end

        # active = @vertices.select {|v| Celluloid::Actor[v].active?}
        active = @vertices2.select {|v| v.active?}

        # futures = active.map { |v| Celluloid::Actor[v].future(:step) }
        # futures.map { |f| f.value }
        # active.each {|v| v.step}
        pmap(active) do |v|
          v.step
        end


        # @active = active.select {|v| Celluloid::Actor[v].active?}.size
        @active = active.select {|v| v.active?}.size
        
      end

      # def each_vertex_value(&block)
      #   @vertices2.each do |v|
      #     yield v.value.class
      #   end
      # end

      def vertex_values
        @vertices2.map { |v| {id: v.id, value: v.value} }
      end
    end # class Worker
  end # module Distributed
end # module Elaine
