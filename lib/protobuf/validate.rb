require 'google/protobuf'

module Google
  module Protobuf
    module MessageExts
      alias_method :orig_to_json, :to_json
      alias_method :orig_to_proto, :to_proto

      def to_json(options = {})
        validate_presence!
        orig_to_json(options)
      end

      def to_proto
        validate_presence!
        orig_to_proto
      end

      def validate_presence!
        self.class.descriptor.entries.each do |entry|
          next if entry.label != :required

          v = self[entry.name]

          validate_fail = case entry.type
          when :int64, :uint64, :int32, :uint32,  :double, :float
            v.nil? || v == 0
          when :string
            v.nil? || v.empty?
          when :enum
            v.nil? || !entry.subtype.entries.map(&:first).include?(v)
          else
            v.nil?
          end

          raise "#{self.class.name}::#{entry.name} was required, but got 「#{v.inspect}」" if validate_fail
        end
      end
    end
  end
end
