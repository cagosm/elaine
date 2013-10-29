require 'celluloid'
module Elaine
  module Distributed
    class Vertex
      include Celluloid
      include Celluloid::Logger

      attr_reader :id
      attr_accessor :value, :messages

      def initialize(id, value, postoffice, *outedges)
        # Might be better to grab post_office dynamically with Celluloid::Actor ?
        @id = id
        @value = value
        @outedges = outedges
        @messages = []
        @active = true
        @superstep = 0
        @postoffice = postoffice
      end

      def edges
        block_given? ? @outedges.each {|e| yield e} : @outedges
      end

      def deliver_to_all_neighbors(msg)
        edges.each {|e| deliver(e, msg)}
      end

      def deliver(to, msg)
        # PostOffice.instance.deliver(to, msg)
        @postoffice.deliver(to, msg)
      end

      def step
        @superstep += 1
        debug "Running super step ##{@superstep}"
        compute
      end

      def halt;     @active = false;  end
      def active!;  @active = true;   end
      def active?;  @active;          end

      def superstep; @superstep; end
      def neighbors; @outedges; end

      def compute; end
    end # class Vertex
  end # module Distributed
end # module Elaine
