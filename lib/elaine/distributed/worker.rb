require 'dcell'

module Elaine
  module Distributed
    class Worker
      include Celluloid

      attr_reader :vertices, :active

      # def initialize(graph = [], id: "elaine-worker", addr: "tcp://127.0.0.1:8096")
      #   raise 'empty worker graph' if graph.empty?
      #   @vertices = graph
      #   @active   = graph.size
      # end

      def initialize(coordinator_node: "elaine.coordinator", g: [], zipcodes: {})

        # @coordinator_node = DCell::Node["elaine.coordinator"]
        @coordinator_node = coordinator_node
        DCell::Node[@coordinator_node][:coordinator].register_worker DCell.me

        Elaine::Distributed::PostOffice.supervise_as :postoffice
        # this could/should probably be done in the constructor
        Celluloid::Actor[:postoffice].zipcodes = zipcodes
        # self.init_graph(g)
      end

      def init_graph(g=[])
         raise 'empty worker graph' if g.empty?

         # we are going to assume that graphs come in as json documents
         # *describing* the graph.
         # @vertices = graph
         # @active   = graph.size

         g.each do |n|
          n[:klazz].supervise_as n[:id], n[:id], n[:value], Celluloid::Actor[:postoffice], n[:outedges]
          @vertices << n[:id]
         end
      end

      def superstep
        # Thread.new do
        @vertices.each do |v|
          vertex = Celluloid::Actor[v]
          # v.messages = PostOffice.instance.read(v.id)
          v.messages = Celluloid::Actor[:postoffice].read!
          v.active! if v.messages.size > 0
        end

        active = @vertices.select {|v| v.active?}
        active.each {|v| v.step}

        @active = active.select {|v| v.active?}.size
        
      end
    end # class Worker
  end # module Distributed
end # module Elaine
