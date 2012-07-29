require 'bundler'
require 'socket'
require 'thread'

Bundler.require

# e = LogStash::Event.new
# e.message = "ERR MER GERD SURKETS"
# e.source = Socket.gethostname

# puts e.to_json

# socket = TCPSocket.new('localhost', 10665)
# socket.write(e.to_hash.to_json)

module Hatchet

  class LogStashFormatter

    # Public: Special formatter that returns a LogStash::Event object.
    #
    def format(level, context, message)
      event = LogStash::Event.new
      event.message = message.to_s
      event.source  = Socket.gethostname
      event['context'] = context.to_s
      event['level']   = level
      event
    end

  end

  class LogStashAppender
    include LevelManager

    attr_accessor :formatter

    attr_accessor :application

    attr_accessor :host

    attr_accessor :port

    def initialize
      @client_lock = Mutex.new
      @formatter = LogStashFormatter.new
      yield self if block_given?
    end

    def add(level, context, message)
      event = @formatter.format(level, context, message)
      event['application'] = @application if @application
      send event
    end

    def stop!
      client.stop!
      client.join
    end

    private

    def send(event)
      client.send "#{event.to_json}\n"
    end

    def client
      return @client if @client
      @client_lock.synchronize do
        @client = SocketClient.new(@host, @port) unless @client
      end
      @client
    end

    class SocketClient

      HALT = :hammer_time

      def initialize(host, port)
        @host = host
        @port = port
        @queue = Queue.new
        @wait = 1
        @sending_thread = run!
      end

      def send(message)
        @queue << message
      end

      def stop!
        send HALT
      end

      def join
        @sending_thread.join
      end

      private

      def run!
        Thread.new { send_messages }
      end

      def send_messages
        until (message = @queue.pop) == HALT
          send_message message
        end
      end

      def send_message(message)
        attempts = 0
        sleep_offset = 1

        begin
          attempts += 1
          socket.write(message)
        rescue
          sleep (sleep_offset / 1024.0)
          sleep_offset = sleep_offset << 1 if sleep_offset < 4096
          # Assume something has gone wrong with the socket so force
          # reconnection.
          @socket = nil
          retry
        end
      end

      def socket
        sleep_offset = 1
        begin
          @socket ||= TCPSocket.new(@host, @port)
        rescue
          sleep (sleep_offset / 1024.0)
          sleep_offset = sleep_offset << 1 if sleep_offset < 4096
          retry
        end
      end

    end

  end

end

foo = Hatchet::LogStashAppender.new do |appender|
  appender.host = 'localhost'
  appender.port = 10665
  appender.application = 'tickle'
end

10.times do
  foo.add :info, Hatchet::LogStashAppender, 'ERR MER GERD SURKETS'
end

foo.stop!

