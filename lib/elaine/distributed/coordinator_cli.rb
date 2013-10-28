require 'thor'
require 'elaine/distributed/coordinator'
require 'securerandom' # for uuid
require 'dcell'

module Elaine
  module Distributed
    class CoordinatorCLI < Thor
      desc "start", "Start a coordinator"
      option :host, type: :string, required: false, default: "127.0.0.1", desc: "The host name to start the coordinator on"
      option :port, type: :numeric, required: false, default: 8095, desc: "The port to start the coordinator on"
      def start
        puts "*" * 20
        puts "STARTING"
        puts "*" * 20

        host = options[:host]
        port = options[:port]
        job_id = SecureRandom.uuid
        # id = 
        # DCell.start id: "elaine.coordinator", addr: "tcp://#{host}:#{port}", registry: {adapter: 'redis', namespace: "elaine-#{job_id}"}
        DCell.start id: "elaine.coordinator", addr: "tcp://#{host}:#{port}"
        supervisor = Elaine::Distributed::Coordinator.supervise_as :coordinator
        trap("INT") {supervisor.terminate; exit }

        # register the service
        # DNSSD.register 'blockless', '_elaine._tcp', job_id, port
        sleep
      end


    end # class CoordinatorCLI
  end # module Distributed
end # module Elaine
