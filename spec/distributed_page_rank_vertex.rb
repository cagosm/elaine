
require 'elaine/distributed'
class DistributedPageRankVertex < Elaine::Distributed::Vertex
  def compute
    puts "Working on supserstep: #{superstep}"
    if superstep >= 1
      sum = messages.inject(0) {|total,msg| total += msg; total }
      @value = (0.15 / 3) + 0.85 * sum
    end

    if superstep < 30
      deliver_to_all_neighbors(@value / neighbors.size)
    else
      halt
    end
  end
end
