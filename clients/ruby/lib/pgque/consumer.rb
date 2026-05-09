# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

require "logger"

module Pgque
  class Consumer
    DEFAULT_MAX_MESSAGES = 2_147_483_647
    WAIT_SLICE_SECONDS = 0.5

    attr_reader :dsn, :queue, :name, :poll_interval, :max_messages,
                :retry_after, :subconsumer, :dead_interval

    attr_accessor :logger

    def initialize(dsn, queue:, name:, poll_interval: 30,
                   max_messages: DEFAULT_MAX_MESSAGES, retry_after: 60,
                   unknown_handler_policy: "nack", subconsumer: nil,
                   dead_interval: nil, logger: nil)
      @dsn = dsn
      @queue = queue
      @name = name
      @poll_interval = poll_interval
      @max_messages = max_messages
      @retry_after = retry_after

      unless ["nack", "ack"].include?(unknown_handler_policy.to_s)
        raise ArgumentError,
              "unknown_handler_policy must be 'nack' or 'ack', " \
              "got #{unknown_handler_policy.inspect}"
      end
      @unknown_handler_policy = unknown_handler_policy.to_s

      if dead_interval && subconsumer.nil?
        raise ArgumentError,
              "dead_interval is only valid in cooperative mode " \
              "(set subconsumer:)"
      end
      @subconsumer = subconsumer
      @dead_interval = dead_interval

      @handlers = {}
      @default_handler = nil
      @running_mutex = Mutex.new
      @running = false
      @logger = logger || default_logger
    end

    def on(event_type, &block)
      raise ArgumentError, "block required for Consumer#on" unless block

      if event_type == "*"
        @default_handler = block
      else
        @handlers[event_type] = block
      end
      block
    end

    def start
      self.running = true

      in_main_thread = (Thread.current == Thread.main)
      original_handlers = {}

      stop_proc = ->(signum) {
        @logger.info("received signal #{signum}, shutting down")
        self.running = false
      }

      if in_main_thread
        ["TERM", "INT"].each do |sig|
          original_handlers[sig] = Signal.trap(sig) { stop_proc.call(sig) }
        end
      end

      begin
        conn = PG.connect(@dsn)
        begin
          channel = "pgque_#{@queue}"
          conn.exec("LISTEN #{conn.escape_identifier(channel)}")
          @logger.info(
            "consumer #{@name} listening on #{@queue} (poll=#{@poll_interval}s)"
          )

          while running?
            poll_once(conn)
            break unless running?
            wait_for_notify_or_stop(conn)
          end
        ensure
          conn.close unless conn.finished?
        end
      ensure
        if in_main_thread
          original_handlers.each { |sig, h| Signal.trap(sig, h || "DEFAULT") }
        end
        @logger.info("consumer #{@name} stopped")
      end
    end

    def stop
      self.running = false
    end

    def running?
      @running_mutex.synchronize { @running }
    end

    # Public for testability; not part of the stable API.
    def poll_once(conn)
      conn.transaction do
        client = Client.new(conn)
        msgs =
          if @subconsumer
            client.receive_coop(
              @queue, @name, @subconsumer,
              max_messages: @max_messages,
              dead_interval: @dead_interval,
            )
          else
            client.receive(@queue, @name, @max_messages)
          end

        next if msgs.empty?

        batch_id = msgs[0].batch_id
        @logger.debug("batch #{batch_id}: #{msgs.size} message(s)")

        nack_failed = dispatch_batch(client, batch_id, msgs)

        next if nack_failed

        rowcount = client.ack(batch_id)
        if rowcount == 0
          @logger.warn(
            "pgque: ack batch #{batch_id} returned 0 -- stale or " \
            "double ack (batch already finished or not found)",
          )
        end
      end
    end

    private

    def dispatch_batch(client, batch_id, msgs)
      nack_failed = false
      msgs.each do |msg|
        handler = @handlers[msg.type] || @default_handler

        if handler.nil?
          if @unknown_handler_policy == "ack"
            @logger.warn(
              "no handler for event type=#{msg.type} ev_id=#{msg.msg_id}; " \
              "acking",
            )
            next
          end
          @logger.warn(
            "no handler for event type=#{msg.type} ev_id=#{msg.msg_id}; " \
            "nacking",
          )
          begin
            client.nack(batch_id, msg, retry_after: @retry_after,
                        reason: "no handler for type=#{msg.type}")
          rescue StandardError => e
            nack_failed = true
            @logger.error(
              "nack failed for unhandled msg_id=#{msg.msg_id}: " \
              "#{e.class}: #{e.message}",
            )
          end
          next
        end

        begin
          handler.call(msg)
        rescue StandardError => e
          @logger.error(
            "handler failed for msg_id=#{msg.msg_id}: " \
            "#{e.class}: #{e.message}",
          )
          begin
            client.nack(batch_id, msg, retry_after: @retry_after)
          rescue StandardError => e2
            nack_failed = true
            @logger.error(
              "nack failed for msg_id=#{msg.msg_id}: " \
              "#{e2.class}: #{e2.message}",
            )
          end
        end
      end
      nack_failed
    end

    def wait_for_notify_or_stop(conn)
      drained = false
      while conn.notifies
        drained = true
      end
      return if drained

      deadline = monotonic + @poll_interval
      while running?
        remaining = deadline - monotonic
        return if remaining <= 0

        slice = [WAIT_SLICE_SECONDS, remaining].min
        notification = conn.wait_for_notify(slice)
        return unless running?

        if notification
          while conn.notifies
            # drain any queued notifications
          end
          return
        end
      end
    end

    def running=(value)
      @running_mutex.synchronize { @running = value }
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # The default logger is effectively silent: it targets $stderr (so
    # messages never collide with application stdout) and ships at level
    # FATAL, which the consumer never emits. Set PGQUE_LOG_LEVEL=warn (or
    # info, debug, error) to see warnings/info from the consumer, or
    # pass logger: Logger.new(...) to Consumer.new for full control.
    def default_logger
      log = Logger.new($stderr)
      log.progname = "pgque.consumer.#{@name}"
      log.level = env_log_level || Logger::FATAL
      log
    end

    def env_log_level
      raw = ENV["PGQUE_LOG_LEVEL"]
      return nil if raw.nil?

      normalized = raw.strip.upcase
      return nil if normalized.empty?

      Logger.const_get(normalized)
    rescue NameError
      nil
    end
  end
end
