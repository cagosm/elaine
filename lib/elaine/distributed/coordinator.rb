require 'dnssd'
require 'celluloid/io'
require 'celluloid/autostart'

module Elaine
  module Distributed
    class Coordinator
      include Celluloid
      # finalizer :shutdown

      attr_reader :workers
      attr_reader :partitions

      # attr_reader :server
      # def initialize(host, port, graph: nil, partitions: 1)
      def initialize(graph: nil, partitions: 1)

        puts "GOT GRAPH: #{graph}"
        #, options = {})
        # raise "empty graph" if graph.empty?

        # @partitions = partitions

        # @workers = []

        # partition(graph) do |subgraph|
        #   @workers << Worker.new(subgraph)
        # end


        # @host = host
        # @port = port

        # puts "*** Starting distributed coordinator on #{host}: #{port}"
        # @server = TCPServer.new host, port
        # async.run
      end

      # def ip_address
      #   @server.local_address.ip_address
      # end

      # def host
      #   @server.local_address.getnameinfo.first
      # end

      # def port
      #   @server.local_address.ip_port
      # end

      # def shutdown
      #   @server.close if @server
      # end

      # def run
      #   loop { async.handle_connection @server.accept }
      # end

      # def handle_connection(socket)
      #   _, port, host = socket.peeraddr

      #   puts "*** Received connection from #{host}:#{port}"
      #   loop { socket.write socket.readpartial(4096) }
      #   # loop { socket.write socket.read }

      # rescue EOFError
      #   puts "*** #{host}:#{port} disconnected"
      #   socket.close
      # end

      def partition(graph)
        size = (graph.size.to_f / partitions).ceil
        graph.each_slice(size) { |slice| yield slice }
      end

      def register_worker(worker_id)
        unless workers.include? DCell::Node[worker_id]
          workers << DCell::Node[worker_id]
        end
      end

      def run_job
        loop do
          # execute a superstep and wait for workers to complete
          step = @workers.select {|w| w.active > 0}.collect {|w| w.superstep }
          # step.each {|t| t.join}

          break if @workers.select {|w| w.active > 0}.size.zero?
        end
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
