require 'elaine/distributed'
class DistributedAddVertex < Elaine::Distributed::Vertex
  def compute
    @value += 1
    halt if @value >= 5
  end
end
