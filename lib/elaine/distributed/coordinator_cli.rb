require 'thor'
require 'elaine/distributed/coordinator'
require 'securerandom' # for uuid

module Elaine
  module Distributed
    class CoordinatorCLI < Thor
      desc "run", "Start a coordinator"
      option :host, type: :string, required: false, default: "localhost", desc: "The host name to start the coordinator on"
      option :port, type: :numeric, required: false, default: 8095, desc: "The port to start the coordinator on"
      def run

        host = options[:host]
        port = options[:port]
        job_id = SecureRandom.uuid

        supervisor = Elaine::Distributed::Coordinator.supervise(host, port)
        trap("INT") { supervisor.terminate; exit }

        # register the service
        DNSSD.register 'blockless', '_elaine._tcp', job_id, port
        sleep
      end
    end # class CoordinatorCLI
  end # module Distributed
end # module Elaine
