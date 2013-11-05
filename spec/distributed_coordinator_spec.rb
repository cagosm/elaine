require 'distributed_helper'

describe Elaine::Distributed::Coordinator do
  # it 'should not allow empty graphs' do
  #   lambda { Coordinator.new([]) }.should raise_error
  # end

  before(:each) do

    TestCoordinator.start
    TestCoordinator.wait_until_ready
    TestWorker1.start
    TestWorker1.wait_until_ready
    TestWorker2.start
    TestWorker2.wait_until_ready
  end

  after(:each) do
    TestWorker1.stop
    TestWorker2.stop
    TestCoordinator.stop
  end

  let(:graph) do
    [
      {
        klazz: DistributedAddVertex,
        id: :igvita,
        value: 1,
        outedges: [:wikipedia]
      },
      {
        klazz: DistributedAddVertex,
        id: :wikipedia,
        value: 2,
        outedges: [:google]
      },
      {
        klazz: DistributedAddVertex,
        id: :google,
        value: 1,
        outedges: [:wikipedia]
      }
    ]
  end


 

  it "should schedule workers to run intil there are no active vertices" do
    
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job
    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      v[:value].should == 5
    end

  end

  it "should calculate PageRank of a circular graph" do
    g =[
      {
        klazz: DistributedPageRankVertex,
        id: :igvita,
        value: 1,
        outedges: [:wikipedia]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :wikipedia,
        value: 1,
        outedges: [:google]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :google,
        value: 1,
        outedges: [:igvita]
      }
    ]

    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    values.each do |v|
      (v[:value] * 100).to_i.should == 33
    end
  end
  

  it "should calculate PageRank of an arbitrary graph" do
    g = [
      # page 1 -> page 1, page 2  (0.18)
      # page 2 -> page 1, page 3  (0.13)
      # page 3 -> page 3          (0.69)

      #                   name     value  out-edges
      {
        klazz: DistributedPageRankVertex,
        id: :igvita,
        value: 1,
        outedges: [:igvita, :wikipedia]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :wikipedia,
        value: 1,
        outedges: [:igvita, :google]
      },
      {
        klazz: DistributedPageRankVertex,
        id: :google,
        value: 1,
        outedges: [:google]
      }
    ]
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = g
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    vertex_values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    vertex_values.each do |v|
      if v[:id] == :igvita
        (v[:value] * 100).ceil.to_i.should == 19
      elsif v[:id] == :wikipedia
        (v[:value] * 100).ceil.to_i.should == 13
      elsif v[:id] == :google
        (v[:value] * 100).to_i.should == 68
      else
        fail "Unexpected node id: #{v[:id]}"
      end
    end

    
  end

  it 'should parition nodes by hashing the node id'
  it 'should allow scheduling multiple partitions to a single worker'
end