module Elaine
  class Vertex
    attr_reader :id
    attr_accessor :value, :messages

    def initialize(id, value, *outedges)
      @id = id
      @value = value
      @outedges = outedges
      @messages = []
      @active = true
      @superstep = 0
    end

    def edges
      block_given? ? @outedges.each {|e| yield e} : @outedges
    end

    def deliver_to_all_neighbors(msg)
      edges.each {|e| deliver(e, msg)}
    end

    def deliver(to, msg)
      PostOffice.instance.deliver(to, msg)
    end

    def step
      @superstep += 1
      compute
    end

    def halt;     @active = false;  end
    def active!;  @active = true;   end
    def active?;  @active;          end

    def superstep; @superstep; end
    def neighbors; @outedges; end

    def compute; end
  end
end
