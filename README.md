# Elaine

Distributed implementation of Google's Pregel framework for graph processing. Forked and modified from https://github.com/igrigorik/pregel.

Elaine provides the components for a distributed pregel installation.

# Requirements

Ruby >= 2.0

I'm moving all my new work to Ruby 2.0 and make heavy use of keyword arguments. Sorry.

Elaine uses [DCell](https://github.com/celluloid/dcell) as the underlying distributed communications library.

DCell has its own set of requirements, but the most prominent is zeromq.


# Is it production ready?

No! Please see the TODO section/

# TODO

There is currently no fault tolerance at all. This can be addressed in several ways, but, the easiest is checkpointing, which is top priority on the todo list.

There is currently one partition per worker. Allowing multiple partitions per worker will likely be accomplished via the creation of a partition actor. The current Worker class will probably be re-factored considerably.

All compute nodes must be brought up manuallly, this includes workers and coordinators. Eventually, I will include funacionality to handle this.

Related to the above, right now, the compute nodes *must* know about the vertex program's class before they are started. This means loading the vertex program in a scope outside of the actual coordinator/workers. As of now, this means you are required to create a separate script that requires the necessary vertex program to start up the coordinator and worker. This is not ideal. I'm hoping to solve it by having the end user supply a packaged gem, and then unpacking it and loading it dynamically.



# Pregel

To learn more about Pregel see following resources:

 * [Pregel, a system for large-scale graph processing](http://portal.acm.org/citation.cfm?id=1582716.1582723)
 * [Large-scale graph computing at Google](http://googleresearch.blogspot.com/2009/06/large-scale-graph-computing-at-google.html)
 * [Phoebus](http://github.com/xslogic/phoebus) is a distributed Erlang implementation of Pregel

# PageRank example with Pregel
To run a PageRank computation on an arbitrary graph, you simply specify the vertices & edges, and then define a compute function for each vertex. The coordinator then partitions the work between a specified number of workers (Ruby threads, in our case), and iteratively executes "supersteps" until we converge on a result. At each superstep, the vertex can read and process incoming messages and then send messages to other vertices. Hence, the full PageRank implementation is:

```ruby
class PageRankVertex < Elaine::Distributed::Vertex
  def compute
    if superstep >= 1
      sum = messages.inject(0) {|total,msg| total += msg; total }
      @value = (0.15 / num_nodes) + 0.85 * sum
    end

    if superstep < 30
      deliver_to_all_neighbors(@value / neighbors.size)
    else
      halt
    end
  end
end
```

The above algorithm will run for 30 iterations, at which point all vertices will mark themselves as inactive and the coordinator will terminate our computation.

 * [Computing PageRank for a simple circular graph](https://github.com/worst/elaine/blob/master/spec/coordinator_spec.rb#L52)
 * [Computing PageRank for a more complex grap](https://github.com/worst/elaine/blob/master/spec/coordinator_spec.rb#L70)

# License

(The MIT License)

Copyright (c) 2010 Jeremy Blackburn

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
