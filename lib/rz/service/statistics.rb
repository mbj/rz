module RZ
  module Service
    module Statistics
      def before_loop
        @s_loops = @s_noops = @s_requests = @s_responses = 0
        @s_loop_start_time = Time.now
        @history = []
        self
      end

      def loop_end
        now = Time.now
        messages = @s_requests + @s_responses
        ms = messages / (now - @s_loop_start_time)
        @history << [now,messages]

        old_time,old_messages = @history.length == 1000 ? @history.slice!(0) : @history.first

        mss = now == old_time ? 0 : (messages - old_messages) / (now - old_time)

        $stderr.puts "msg: req: %03d rep: %03d noop: %03d miss: %03d m/s: %0.2f ms/s: %0.2f" % [@s_requests,@s_responses,@s_noops,@s_requests - @s_requests,ms,mss]
        self
      end

      attr_reader :short_message_count,:short_time

      def noop
        @s_noops +=1
        self
      end

      def request
        @s_requests +=1
        self
      end

      def response
        @s_responses +=1
        self
      end

      def loop_start
        @s_loops += 1
        self
      end

      def self.included(base)
        base.send :hook,:before_loop
        base.send :hook,:loop_start
        base.send :hook,:loop_end
        base.send :hook,:noop
        base.send :hook,:response
        base.send :hook,:request
      end
    end
  end
end
