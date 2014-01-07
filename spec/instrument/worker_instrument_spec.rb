require 'instrument/helpers/coordinator_instruments_helper'
require 'elaine/instrument'
require 'elaine/instrument/worker_instrument'



describe Elaine::Instrument::WorkerInstrument do

  before(:each) do
    CoordinatorInstrumentCoordinator.start
    CoordinatorInstrumentCoordinator.wait_until_ready
    CoordinatorInstrumentWorker1.start
    CoordinatorInstrumentWorker1.wait_until_ready
    CoordinatorInstrumentWorker2.start
    CoordinatorInstrumentWorker2.wait_until_ready
  end

  after(:each) do
    CoordinatorInstrumentWorker1.stop
    CoordinatorInstrumentWorker2.stop
    CoordinatorInstrumentCoordinator.stop
  end

  let(:graph) do
    [
      {
        klazz: InstrumentVertex,
        # id: :igvita,
        id: 0,
        value: 1,
        outedges: [1]
      },
      {
        klazz: InstrumentVertex,
        # id: :wikipedia,
        id: 1,
        value: 2,
        outedges: [2]
      },
      {
        klazz: InstrumentVertex,
        # id: :google,
        id: 2,
        value: 1,
        outedges: [1]
      }
    ]
  end

  it "should properly record the number of supersteps" do
    puts "wtf?"
    # DCell::Node["test.elaine.coordinator"][:coordinator].instrumented?.should be_true
    # DCell::Node["test.elaine.coordinator"][:coordinator].i_exist?.should be_true

    # puts "setting graph"
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    # puts "done setting graph"
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    # measurements = DCell::Node["test.elaine.coordinator"][:coordinator].instrument_measurements
    DCell::Node["test.elaine.worker1"][:worker].instrumented?.should be_true
    DCell::Node["test.elaine.worker1"][:worker].instrument_measurements["superstep"][:count].should == 5
    DCell::Node["test.elaine.worker2"][:worker].instrument_measurements["superstep"][:count].should == 5
    # num_supersteps = measurements["superstep"].size
    # num_supersteps.should == 5

    # step_time = num_supersteps * 0.1 # keep in mind that we are running with 3 threads per worker
    # measurements["superstep"].reduce(:+).should be_within(0.2).of(step_time)



    # values = DCell::Node["test.elaine.coordinator"][:coordinator].vertex_values
    # values.each do |v|
    #   v[:value].should == 5
    # end

  end

  it "should properly record the elapsed time" do

    puts "blah"
    DCell::Node["test.elaine.coordinator"][:coordinator].graph = graph
    # puts "done setting graph"
    DCell::Node["test.elaine.coordinator"][:coordinator].partition
    DCell::Node["test.elaine.coordinator"][:coordinator].run_job

    # measurements = DCell::Node["test.elaine.coordinator"][:coordinator].instrument_measurements
    step_time = 0.5
    
    measure = DCell::Node["test.elaine.worker1"][:worker].instrument_measurements
    measure["superstep"][:total_time].should be_within(0.05).of(step_time)
    
    measure = DCell::Node["test.elaine.worker2"][:worker].instrument_measurements
    measure["superstep"][:total_time].should be_within(0.05).of(step_time)


  end

  
end