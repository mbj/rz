module RZ
  module Service
    module Statistics
      def before_loop
        @s_loops = @s_noops = @s_requests = @s_responses = 0
        @s_loop_start_time = Time.now
        @history = []
        self
      end

      def count_loop_end
        now = Time.now
        messages = @s_requests + @s_responses
        ms = messages / (now - @s_loop_start_time)
        @history << [now,messages]

        old_time,old_messages = 
          if @history.length == 1000 
            @history.slice!(0) 
          else
            @history.first
          end

        mss = now == old_time ? 0 : (messages - old_messages) / (now - old_time)

        $stderr.puts "msg: req: %03d rep: %03d noop: %03d miss: %03d m/s: %0.2f ms/s: %0.2f" % [@s_requests,@s_responses,@s_noops,@s_requests - @s_responses,ms,mss]

        self
      end

      attr_reader :short_message_count,:short_time

      def count_noop
        @s_noops +=1

        self
      end

      def count_request
        @s_requests +=1

        self
      end

      def count_response
        @s_responses +=1

        self
      end

      def count_loop_start
        @s_loops += 1

        self
      end

      def self.included(base)
        base.send :hook,:before_loop
        base.send :hook,:loop_start, :count_loop_start
        base.send :hook,:loop_end,   :count_loop_end
        base.send :hook,:noop,       :count_noop
        base.send :hook,:response,   :count_response
        base.send :hook,:request,    :count_request
      end
    end
  end
end
