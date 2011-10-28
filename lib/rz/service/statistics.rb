module RZ
  module Service
    module Statistics
      def before_loop
        @s_loops = @s_noops = @s_requests = @s_responses = 0
        @s_loop_start_time ||= Time.now
        reset_short_stats
      end

      def loop_end
        reset_short_stats if (@s_loops % 100).zero?
        print_stats
      end

      attr_reader :short_message_count,:short_time

      def reset_short_stats
        @s_short_time = Time.now
        @s_short_message_count = @s_requests + @s_responses
      end

      def print_stats
        messages = @s_requests + @s_responses
        ms = messages / (Time.now - @s_loop_start_time)
        short_messages = messages - @s_short_message_count
        mss = short_messages / (Time.now - @s_short_time)
        $stderr.puts "msg: req: %03d rep: %03d noop: %03d miss: %03d m/s: %0.2f ms/s: %0.2f" % [@s_requests,@s_responses,@s_noops,@s_requests - @s_requests,ms,mss]
        self
      end
     
      def noop
        @s_noops +=1
      end

      def request
        @s_requests +=1
      end

      def response
        @s_responses +=1
      end

      def loop_start
        @s_loops += 1
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
