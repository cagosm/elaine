module Elaine
  module Distributed
    class Worker
      include Celluloid

      attr_reader :vertices, :active

      def initialize(graph = [], id: "elaine-worker", addr: "tcp://127.0.0.1:8096")
        raise 'empty worker graph' if graph.empty?
        @vertices = graph
        @active   = graph.size
      end

      def superstep
        # Thread.new do
        @vertices.each do |v|
          v.messages = PostOffice.instance.read(v.id)
          v.active! if v.messages.size > 0
        end

        active = @vertices.select {|v| v.active?}
        active.each {|v| v.step}

        @active = active.select {|v| v.active?}.size
        
      end
    end # class Worker
  end # module Distributed
end # module Elaine
