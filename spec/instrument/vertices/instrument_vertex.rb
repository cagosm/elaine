
require 'elaine/distributed'
class InstrumentVertex < Elaine::Distributed::Vertex
  def compute
    @value += 1
    sleep(0.1)
    vote_to_stop if @value > 5
    # # puts "Working on supserstep: #{superstep}"
    # if superstep >= 1
    #   # sum = messages.inject(0) {|total,msg| total += msg; total }
    #   # sum = messages.reduce(0, :+)
    #   sum = messages.reduce(0) do |total, msg|
    #     raise "Got an out-of-step message! current superstep: #{superstep}, message from superstep: #{msg[:superstep]}, msg: #{msg}" if msg[:superstep] != (superstep - 1)
    #     total += msg[:value]

    #   end
    #   @value = (0.15 / 3) + 0.85 * sum
    # end

    # if superstep < 30
    #   deliver_to_all_neighbors({value: @value / neighbors.size, superstep: superstep})
    # else
    #   halt
    # end
  end
end
