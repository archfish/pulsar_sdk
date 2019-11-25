module PulsarSdk
  module Producer
    class Router
      def initialize(scheme = :string_hash)
        case scheme.to_sym
        when :string_hash
          @handler = string_hash
        when :murmur_hash
          @handler = murmur3_hash
        else
          raise "Unknown hash scheme #{scheme}"
        end
      end

      def route(key, total, delay = 0)
        return 0 if total <= 1

        return (@handler.call(key) % total) unless key.to_s.empty?

        Murmur3.int32_hash(TimeX.now.timestamp) % total
      end

      # 将hash值限制在32位内，防止key过长导致过多内存占用
      def string_hash
        max_mod = 1 << 32
        Proc.new do |key|
          h = 0
          key.to_s.each_byte do |x|
            h = (31*h)%max_mod + x
          end

          h
        end
      end

      def murmur3_hash
        Proc.new do |key, seed = 31|
          # Using 0x7fffffff was maintain compatibility with values used in Java client
          Murmur3.hash(key, seed) & 0x7fffffff
        end
      end
    end

    # cpoy from https://github.com/funny-falcon/murmurhash3-ruby
    module Murmur3
      extend self

      MASK32 = 0xffffffff

      def int32_hash(i, seed=0)
        hash([i].pack("V"), seed)
      end

      def hash(str, seed=0)
        h1 = seed
        numbers = str.unpack('V*C*')
        tailn = str.bytesize % 4
        tail = numbers.slice!(numbers.size - tailn, tailn)
        for k1 in numbers
          h1 ^= mmix(k1)
          h1 = rotl(h1, 13)
          h1 = (h1*5 + 0xe6546b64) & MASK32
        end

        unless tail.empty?
          k1 = 0
          tail.reverse_each do |c1|
            k1 = (k1 << 8) | c1
          end
          h1 ^= mmix(k1)
        end

        h1 ^= str.bytesize
        fmix(h1)
      end

      private
      def rotl(x, r)
        ((x << r) | (x >> (32 - r))) & MASK32
      end

      def fmix(h)
        h &= MASK32
        h ^= h >> 16
        h = (h * 0x85ebca6b) & MASK32
        h ^= h >> 13
        h = (h * 0xc2b2ae35) & MASK32
        h ^ (h >> 16)
      end

      def mmix(k1)
        k1 = (k1 * 0xcc9e2d51) & MASK32
        k1 = rotl(k1, 15)
        (k1 * 0x1b873593) & MASK32
      end
    end
  end
end
