require 'dnssd'
require 'celluloid/io'
require 'celluloid/autostart'

module Elaine
  module Distributed
    class Coordinator
      include Celluloid::IO
      finalizer :shutdown

      attr_reader :workers
      attr_reader :partitions

      attr_reader :server
      def initialize(graph, partitions: 1, host: "localhost", port: 8095) 
        #, options = {})
        # raise "empty graph" if graph.empty?

        # @partitions = partitions

        # @workers = []

        # partition(graph) do |subgraph|
        #   @workers << Worker.new(subgraph)
        # end


        # @host = host
        # @port = port

        puts "*** Starting distributed coordinator on #{host}: #{port}"
        @server = TCPServer.new host, port
        async.run
      end

      def ip_address
        @server.local_address.ip_address
      end

      def host
        @server.local_address.getnameinfo.first
      end

      def port
        @server.local_address.ip_port
      end

      def shutdown
        @server.close if @server
      end

      def run
        loop { async.handle_connection @server.accept }
      end

      def handle_connection(socket)
        _, port, host = socket.peeraddr

        puts "*** Received connection from #{host}:#{port}"
        loop { socket.write socket.readpartial(4096) }

      rescue EOFError
        puts "*** #{host}:#{port} disconnected"
        socket.close
      end

      def partition(graph)
        size = (graph.size.to_f / partitions).ceil
        graph.each_slice(size) { |slice| yield slice }
      end

      def run
        loop do
          # execute a superstep and wait for workers to complete
          step = @workers.select {|w| w.active > 0}.collect {|w| w.superstep }
          step.each {|t| t.join}

          break if @workers.select {|w| w.active > 0}.size.zero?
        end
      end

    end # class Coordinator
  end # module Distributed
end # module Elaine
