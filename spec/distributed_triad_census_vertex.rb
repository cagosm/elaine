
require 'elaine/distributed'
class DistributedTriadCensusVertex < Elaine::Distributed::Vertex
  def compute
    # puts "Working on supserstep: #{superstep}"
    if superstep == 1
      deliver_to_all_neighbors({source: id; neighborhood: @outedges})
    elsif superstep == 2
      # sum = messages.inject(0) {|total,msg| total += msg; total }
      # sum = messages.reduce(0, :+)
      @value = {type2: 0, type3: 0 }

      u = id.to_s.plit("_")[1].to_i 
      messages.each(0) do |msg|
        # raise "Got an out-of-step message! current superstep: #{superstep}, message from superstep: #{msg[:superstep]}" if msg[:superstep] != (superstep - 1)  
        v = msg[:source].to_s.split("_")[1].to_i
        if u < v
          
          msg[:neighborhood].each do |node_w|
            w = node_w.to_s.split("_")[1].to_i
            if v < w
              num_edges = 2
              if @outedges.include? node_w
                num_edges += 1
              end

              @value[:type2] += 1 if num_edges == 2
              @value[:type3] += 1 if num_edges == 3 

            end
          end
        end
      end
    
    else
      vote_to_stop
    end

    
  end
end
